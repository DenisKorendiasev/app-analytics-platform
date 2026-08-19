package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestHandlerIngest(t *testing.T) {
	service := &handlerServiceStub{
		ingest: func(_ context.Context, input Input) (Event, error) {
			want := validInput()
			if input != want {
				t.Errorf("Ingest() input = %+v, want %+v", input, want)
			}
			return Event{EventID: uuid.New()}, nil
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(validRequestBody()))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusAccepted)
	}
	if response.Body.Len() != 0 {
		t.Errorf("response body = %q, want empty body", response.Body.String())
	}
}

func TestHandlerIngestInvalidJSON(t *testing.T) {
	service := &handlerServiceStub{
		ingest: func(_ context.Context, _ Input) (Event, error) {
			t.Fatal("Ingest() called for invalid JSON")
			return Event{}, nil
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{"app_id":`))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertEventErrorResponse(t, response, http.StatusBadRequest, "invalid request body")
}

func TestHandlerIngestUnknownApp(t *testing.T) {
	service := &handlerServiceStub{
		ingest: func(_ context.Context, _ Input) (Event, error) {
			return Event{}, ErrAppNotFound
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(validRequestBody()))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertEventErrorResponse(t, response, http.StatusNotFound, ErrAppNotFound.Error())
}

func TestHandlerIngestInvalidEvent(t *testing.T) {
	tests := []struct {
		name         string
		serviceError error
		wantMessage  string
	}{
		{name: "type", serviceError: ErrEventTypeInvalid, wantMessage: ErrEventTypeInvalid.Error()},
		{name: "platform", serviceError: ErrPlatformInvalid, wantMessage: ErrPlatformInvalid.Error()},
		{name: "purchase revenue", serviceError: ErrPurchaseRevenueInvalid, wantMessage: ErrPurchaseRevenueInvalid.Error()},
		{name: "wrapped validation error", serviceError: fmt.Errorf("validate event: %w", ErrEventTypeInvalid), wantMessage: ErrEventTypeInvalid.Error()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &handlerServiceStub{
				ingest: func(_ context.Context, _ Input) (Event, error) {
					return Event{}, tt.serviceError
				},
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(validRequestBody()))
			response := httptest.NewRecorder()

			newHandlerForTest(service).ServeHTTP(response, request)

			assertEventErrorResponse(t, response, http.StatusBadRequest, tt.wantMessage)
		})
	}
}

func TestHandlerIngestInternalError(t *testing.T) {
	service := &handlerServiceStub{
		ingest: func(_ context.Context, _ Input) (Event, error) {
			return Event{}, errors.New("RabbitMQ unavailable")
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(validRequestBody()))
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertEventErrorResponse(t, response, http.StatusInternalServerError, "internal server error")
}

func newHandlerForTest(service HandlerService) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(service, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

func assertEventErrorResponse(t *testing.T, response *httptest.ResponseRecorder, status int, message string) {
	t.Helper()

	if response.Code != status {
		t.Fatalf("status code = %d, want %d", response.Code, status)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var body errorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Error != message {
		t.Errorf("error = %q, want %q", body.Error, message)
	}
}

func validRequestBody() string {
	return `{"app_id":"b8edbe8d-4fa6-42fd-a351-9a98d17d8b83","event_type":"purchase","country":"RS","platform":"android","revenue_cents":999,"timestamp":"2026-08-18T12:35:02Z"}`
}

type handlerServiceStub struct {
	ingest func(context.Context, Input) (Event, error)
}

func (s *handlerServiceStub) Ingest(ctx context.Context, input Input) (Event, error) {
	if s.ingest == nil {
		return Event{}, errors.New("unexpected Ingest call")
	}
	return s.ingest(ctx, input)
}
