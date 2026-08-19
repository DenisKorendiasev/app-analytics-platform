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

func TestRankingsHandlerGetApplicationRankings(t *testing.T) {
	want := RankingResult{
		Metric: RankingMetricInstalls,
		Items: []Ranking{
			{AppID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), Value: 12},
			{AppID: uuid.MustParse("22222222-2222-4222-8222-222222222222"), Value: 7},
		},
	}
	service := &rankingsHandlerServiceStub{rankings: func(_ context.Context, input RankingInput) (RankingResult, error) {
		wantInput := RankingInput{Metric: "installs", Country: "RS", From: "2026-08-01", To: "2026-08-18", Limit: "2"}
		if input != wantInput {
			t.Errorf("GetApplicationRankings() input = %+v, want %+v", input, wantInput)
		}
		return want, nil
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/rankings?metric=installs&country=RS&from=2026-08-01&to=2026-08-18&limit=2", nil)
	response := httptest.NewRecorder()

	newRankingsHandlerForTest(service).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var got rankingsResponse
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	wantResponse := newRankingsResponse(want)
	if got.Metric != wantResponse.Metric || len(got.Rankings) != len(wantResponse.Rankings) {
		t.Fatalf("response = %+v, want %+v", got, wantResponse)
	}
	for index := range wantResponse.Rankings {
		if got.Rankings[index] != wantResponse.Rankings[index] {
			t.Errorf("ranking %d = %+v, want %+v", index, got.Rankings[index], wantResponse.Rankings[index])
		}
	}
}

func TestRankingsHandlerValidationErrors(t *testing.T) {
	tests := []struct {
		name       string
		serviceErr error
		want       string
	}{
		{name: "metric", serviceErr: fmt.Errorf("validate: %w", ErrMetricInvalid), want: ErrMetricInvalid.Error()},
		{name: "from", serviceErr: fmt.Errorf("validate: %w", ErrFromInvalid), want: ErrFromInvalid.Error()},
		{name: "to", serviceErr: fmt.Errorf("validate: %w", ErrToInvalid), want: ErrToInvalid.Error()},
		{name: "date range", serviceErr: fmt.Errorf("validate: %w", ErrDateRangeInvalid), want: ErrDateRangeInvalid.Error()},
		{name: "limit", serviceErr: fmt.Errorf("validate: %w", ErrLimitInvalid), want: ErrLimitInvalid.Error()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &rankingsHandlerServiceStub{rankings: func(context.Context, RankingInput) (RankingResult, error) {
				return RankingResult{}, tt.serviceErr
			}}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/rankings", nil)
			response := httptest.NewRecorder()

			newRankingsHandlerForTest(service).ServeHTTP(response, request)

			assertAnalyticsErrorResponse(t, response, http.StatusBadRequest, tt.want)
		})
	}
}

func TestRankingsHandlerInternalError(t *testing.T) {
	service := &rankingsHandlerServiceStub{rankings: func(context.Context, RankingInput) (RankingResult, error) {
		return RankingResult{}, errors.New("ClickHouse unavailable")
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/rankings", nil)
	response := httptest.NewRecorder()

	newRankingsHandlerForTest(service).ServeHTTP(response, request)

	assertAnalyticsErrorResponse(t, response, http.StatusInternalServerError, "internal server error")
}

func newRankingsHandlerForTest(service RankingsHandlerService) http.Handler {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewRankingsHandler(service, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

type rankingsHandlerServiceStub struct {
	rankings func(context.Context, RankingInput) (RankingResult, error)
}

func (s *rankingsHandlerServiceStub) GetApplicationRankings(ctx context.Context, input RankingInput) (RankingResult, error) {
	if s.rankings == nil {
		return RankingResult{}, errors.New("unexpected GetApplicationRankings call")
	}
	return s.rankings(ctx, input)
}
