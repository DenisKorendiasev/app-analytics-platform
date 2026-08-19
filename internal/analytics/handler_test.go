package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestHandlerGetApplicationStatistics(t *testing.T) {
	appID := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	want := Statistics{AppID: appID, Installs: 12, Sessions: 34, Purchases: 5, RevenueCents: 6789}
	service := &handlerServiceStub{statistics: func(_ context.Context, gotAppID uuid.UUID, input FilterInput) (Statistics, error) {
		if gotAppID != appID {
			t.Errorf("GetApplicationStatistics() app ID = %s, want %s", gotAppID, appID)
		}
		wantInput := FilterInput{From: "2026-08-01", To: "2026-08-18", Country: "RS", Platform: "android"}
		if input != wantInput {
			t.Errorf("GetApplicationStatistics() input = %+v, want %+v", input, wantInput)
		}
		return want, nil
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/"+appID.String()+"/stats?from=2026-08-01&to=2026-08-18&country=RS&platform=android", nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var got statisticsResponse
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got != newStatisticsResponse(want) {
		t.Errorf("response = %+v, want %+v", got, newStatisticsResponse(want))
	}
}

func TestHandlerGetApplicationStatisticsInvalidAppID(t *testing.T) {
	service := &handlerServiceStub{statistics: func(context.Context, uuid.UUID, FilterInput) (Statistics, error) {
		t.Fatal("GetApplicationStatistics() called for invalid UUID")
		return Statistics{}, nil
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/not-a-uuid/stats", nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertAnalyticsErrorResponse(t, response, http.StatusBadRequest, "invalid app id")
}

func TestHandlerGetApplicationStatisticsValidationErrors(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr error
		want       string
	}{
		{name: "app ID", serviceErr: fmt.Errorf("validate: %w", ErrAppIDRequired), want: ErrAppIDRequired.Error()},
		{name: "from", serviceErr: fmt.Errorf("validate: %w", ErrFromInvalid), want: ErrFromInvalid.Error()},
		{name: "to", serviceErr: fmt.Errorf("validate: %w", ErrToInvalid), want: ErrToInvalid.Error()},
		{name: "date range", serviceErr: fmt.Errorf("validate: %w", ErrDateRangeInvalid), want: ErrDateRangeInvalid.Error()},
		{name: "platform", serviceErr: fmt.Errorf("validate: %w", ErrPlatformInvalid), want: ErrPlatformInvalid.Error()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &handlerServiceStub{statistics: func(context.Context, uuid.UUID, FilterInput) (Statistics, error) {
				return Statistics{}, tt.serviceErr
			}}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/"+uuid.NewString()+"/stats", nil)
			response := httptest.NewRecorder()

			newHandlerForTest(service).ServeHTTP(response, request)

			assertAnalyticsErrorResponse(t, response, http.StatusBadRequest, tt.want)
		})
	}
}

func TestHandlerGetApplicationStatisticsInternalError(t *testing.T) {
	service := &handlerServiceStub{statistics: func(context.Context, uuid.UUID, FilterInput) (Statistics, error) {
		return Statistics{}, errors.New("ClickHouse unavailable")
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/apps/"+uuid.NewString()+"/stats", nil)
	response := httptest.NewRecorder()

	newHandlerForTest(service).ServeHTTP(response, request)

	assertAnalyticsErrorResponse(t, response, http.StatusInternalServerError, "internal server error")
}

func newHandlerForTest(service HandlerService) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(service, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

func assertAnalyticsErrorResponse(t *testing.T, response *httptest.ResponseRecorder, status int, message string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, status, response.Body)
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

type handlerServiceStub struct {
	statistics func(context.Context, uuid.UUID, FilterInput) (Statistics, error)
}

func (s *handlerServiceStub) GetApplicationStatistics(ctx context.Context, appID uuid.UUID, input FilterInput) (Statistics, error) {
	if s.statistics == nil {
		return Statistics{}, errors.New("unexpected GetApplicationStatistics call")
	}
	return s.statistics(ctx, appID, input)
}
