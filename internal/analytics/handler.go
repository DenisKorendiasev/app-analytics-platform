package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

// HandlerService defines the statistics operation used by the HTTP adapter.
type HandlerService interface {
	GetApplicationStatistics(ctx context.Context, appID uuid.UUID, input FilterInput) (Statistics, error)
}

var _ HandlerService = (*Service)(nil)

// Handler exposes application statistics over HTTP.
type Handler struct {
	service HandlerService
	logger  *slog.Logger
}

// NewHandler creates an analytics HTTP Handler.
func NewHandler(service HandlerService, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger}
}

// RegisterRoutes registers analytics HTTP routes.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/apps/{id}/stats", h.getApplicationStatistics)
}

func (h *Handler) getApplicationStatistics(w http.ResponseWriter, r *http.Request) {
	appID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid app id")
		return
	}

	query := r.URL.Query()
	statistics, err := h.service.GetApplicationStatistics(r.Context(), appID, FilterInput{
		From:     query.Get("from"),
		To:       query.Get("to"),
		Country:  query.Get("country"),
		Platform: query.Get("platform"),
	})
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	if err := writeJSON(w, http.StatusOK, newStatisticsResponse(statistics)); err != nil {
		h.logger.Error("write application statistics response", "app_id", appID, "error", err)
	}
}

func (h *Handler) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrAppIDRequired):
		h.writeError(w, http.StatusBadRequest, ErrAppIDRequired.Error())
	case errors.Is(err, ErrFromInvalid):
		h.writeError(w, http.StatusBadRequest, ErrFromInvalid.Error())
	case errors.Is(err, ErrToInvalid):
		h.writeError(w, http.StatusBadRequest, ErrToInvalid.Error())
	case errors.Is(err, ErrDateRangeInvalid):
		h.writeError(w, http.StatusBadRequest, ErrDateRangeInvalid.Error())
	case errors.Is(err, ErrPlatformInvalid):
		h.writeError(w, http.StatusBadRequest, ErrPlatformInvalid.Error())
	default:
		h.logger.Error("application statistics request failed", "path", r.URL.Path, "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	if err := writeJSON(w, status, errorResponse{Error: message}); err != nil {
		h.logger.Error("write analytics error response", "status", status, "error", err)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

type statisticsResponse struct {
	AppID        uuid.UUID `json:"app_id"`
	Installs     uint64    `json:"installs"`
	Sessions     uint64    `json:"sessions"`
	Purchases    uint64    `json:"purchases"`
	RevenueCents int64     `json:"revenue_cents"`
}

func newStatisticsResponse(statistics Statistics) statisticsResponse {
	return statisticsResponse(statistics)
}

type errorResponse struct {
	Error string `json:"error"`
}
