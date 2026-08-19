package analytics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

// RankingsHandlerService defines the rankings operation used by the HTTP adapter.
type RankingsHandlerService interface {
	GetApplicationRankings(ctx context.Context, input RankingInput) (RankingResult, error)
}

var _ RankingsHandlerService = (*RankingsService)(nil)

// RankingsHandler exposes application rankings over HTTP.
type RankingsHandler struct {
	service RankingsHandlerService
	logger  *slog.Logger
}

// NewRankingsHandler creates a Rankings API handler.
func NewRankingsHandler(service RankingsHandlerService, logger *slog.Logger) *RankingsHandler {
	return &RankingsHandler{service: service, logger: logger}
}

// RegisterRoutes registers the Rankings API route.
func (h *RankingsHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/rankings", h.getApplicationRankings)
}

func (h *RankingsHandler) getApplicationRankings(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	result, err := h.service.GetApplicationRankings(r.Context(), RankingInput{
		Metric:  query.Get("metric"),
		Country: query.Get("country"),
		From:    query.Get("from"),
		To:      query.Get("to"),
		Limit:   query.Get("limit"),
	})
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	if err := writeJSON(w, http.StatusOK, newRankingsResponse(result)); err != nil {
		h.logger.Error("write application rankings response", "error", err)
	}
}

func (h *RankingsHandler) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrMetricInvalid):
		h.writeError(w, http.StatusBadRequest, ErrMetricInvalid.Error())
	case errors.Is(err, ErrFromInvalid):
		h.writeError(w, http.StatusBadRequest, ErrFromInvalid.Error())
	case errors.Is(err, ErrToInvalid):
		h.writeError(w, http.StatusBadRequest, ErrToInvalid.Error())
	case errors.Is(err, ErrDateRangeInvalid):
		h.writeError(w, http.StatusBadRequest, ErrDateRangeInvalid.Error())
	case errors.Is(err, ErrLimitInvalid):
		h.writeError(w, http.StatusBadRequest, ErrLimitInvalid.Error())
	default:
		h.logger.Error("application rankings request failed", "path", r.URL.Path, "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (h *RankingsHandler) writeError(w http.ResponseWriter, status int, message string) {
	if err := writeJSON(w, status, errorResponse{Error: message}); err != nil {
		h.logger.Error("write rankings error response", "status", status, "error", err)
	}
}

type rankingsResponse struct {
	Metric   RankingMetric   `json:"metric"`
	Rankings []rankingRecord `json:"rankings"`
}

type rankingRecord struct {
	AppID uuid.UUID `json:"app_id"`
	Value uint64    `json:"value"`
}

func newRankingsResponse(result RankingResult) rankingsResponse {
	records := make([]rankingRecord, len(result.Items))
	for index, item := range result.Items {
		records[index] = rankingRecord{AppID: item.AppID, Value: item.Value}
	}
	return rankingsResponse{Metric: result.Metric, Rankings: records}
}
