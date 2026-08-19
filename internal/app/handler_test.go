package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHandlerCreate(t *testing.T) {
	application := testApp()
	service := &handlerServiceStub{
		create: func(_ context.Context, name, publisher, category string) (App, error) {
			if name != "Spotify" || publisher != "Spotify AB" || category != "music" {
				t.Errorf("Create() input = %q, %q, %q", name, publisher, category)
			}
			return application, nil
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(`{"name":"Spotify","publisher":"Spotify AB","category":"music"}`))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusCreated)
	}
	assertJSONContentType(t, response)
	if location := response.Header().Get("Location"); location != "/api/v1/apps/"+application.ID.String() {
		t.Errorf("Location = %q, want application URL", location)
	}
	var body appResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body != newAppResponse(application) {
		t.Errorf("response = %+v, want %+v", body, newAppResponse(application))
	}
}

func TestHandlerCreateInvalidJSON(t *testing.T) {
	service := &handlerServiceStub{
		create: func(_ context.Context, _, _, _ string) (App, error) {
			t.Fatal("Create() called for invalid JSON")
			return App{}, nil
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(`{"name":`))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusBadRequest, "invalid request body")
}

func TestHandlerCreateMissingRequiredField(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		serviceError error
	}{
		{name: "name", body: `{"publisher":"Spotify AB","category":"music"}`, serviceError: ErrNameRequired},
		{name: "publisher", body: `{"name":"Spotify","category":"music"}`, serviceError: ErrPublisherRequired},
		{name: "category", body: `{"name":"Spotify","publisher":"Spotify AB"}`, serviceError: ErrCategoryRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &handlerServiceStub{
				create: func(_ context.Context, _, _, _ string) (App, error) {
					return App{}, tt.serviceError
				},
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(tt.body))
			response := httptest.NewRecorder()

			newHandlerForTest(service).ServeHTTP(response, request)

			assertErrorResponse(t, response, http.StatusBadRequest, tt.serviceError.Error())
		})
	}
}

func TestHandlerCreateInternalError(t *testing.T) {
	service := &handlerServiceStub{
		create: func(_ context.Context, _, _, _ string) (App, error) {
			return App{}, errors.New("database connection failed")
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/apps", strings.NewReader(`{"name":"Spotify","publisher":"Spotify AB","category":"music"}`))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusInternalServerError, "internal server error")
}

func TestHandlerGetByID(t *testing.T) {
	application := testApp()
	service := &handlerServiceStub{
		getByID: func(_ context.Context, id uuid.UUID) (App, error) {
			if id != application.ID {
				t.Errorf("GetByID() ID = %s, want %s", id, application.ID)
			}
			return application, nil
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/"+application.ID.String(), nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	assertJSONContentType(t, response)
	var body appResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body != newAppResponse(application) {
		t.Errorf("response = %+v, want %+v", body, newAppResponse(application))
	}
}

func TestHandlerGetByIDNotFound(t *testing.T) {
	id := testApp().ID
	service := &handlerServiceStub{
		getByID: func(_ context.Context, _ uuid.UUID) (App, error) {
			return App{}, ErrNotFound
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/"+id.String(), nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusNotFound, ErrNotFound.Error())
}

func TestHandlerGetByIDInvalidUUID(t *testing.T) {
	service := &handlerServiceStub{
		getByID: func(_ context.Context, _ uuid.UUID) (App, error) {
			t.Fatal("GetByID() called for invalid UUID")
			return App{}, nil
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/not-a-uuid", nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertErrorResponse(t, response, http.StatusBadRequest, "invalid app id")
}

func newHandlerForTest(service HandlerService) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(service, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

func assertErrorResponse(t *testing.T, response *httptest.ResponseRecorder, status int, message string) {
	t.Helper()

	if response.Code != status {
		t.Fatalf("status code = %d, want %d", response.Code, status)
	}
	assertJSONContentType(t, response)
	var body errorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Error != message {
		t.Errorf("error = %q, want %q", body.Error, message)
	}
}

func assertJSONContentType(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()

	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
}

func testApp() App {
	return App{
		ID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		Name:      "Spotify",
		Publisher: "Spotify AB",
		Category:  "music",
		CreatedAt: time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC),
	}
}

type handlerServiceStub struct {
	create  func(context.Context, string, string, string) (App, error)
	getByID func(context.Context, uuid.UUID) (App, error)
}

func (s *handlerServiceStub) Create(ctx context.Context, name, publisher, category string) (App, error) {
	if s.create == nil {
		return App{}, errors.New("unexpected Create call")
	}
	return s.create(ctx, name, publisher, category)
}

func (s *handlerServiceStub) GetByID(ctx context.Context, id uuid.UUID) (App, error) {
	if s.getByID == nil {
		return App{}, errors.New("unexpected GetByID call")
	}
	return s.getByID(ctx, id)
}
