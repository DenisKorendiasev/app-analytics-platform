package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const maxRequestBodySize = 1 << 20

// HandlerService defines Event operations used by the HTTP adapter.
type HandlerService interface {
	Ingest(ctx context.Context, input Input) (Event, error)
}

var _ HandlerService = (*Service)(nil)

// Handler exposes event ingestion over HTTP.
type Handler struct {
	service HandlerService
	logger  *slog.Logger
}

// NewHandler creates an Event HTTP handler.
func NewHandler(service HandlerService, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger}
}

// RegisterRoutes registers Event HTTP routes on a server mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/events", h.ingest)
}

func (h *Handler) ingest(w http.ResponseWriter, r *http.Request) {
	var request ingestEventRequest
	if err := decodeJSON(w, r, &request); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	_, err := h.service.Ingest(r.Context(), Input(request))
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrAppIDRequired):
		h.writeError(w, http.StatusBadRequest, ErrAppIDRequired.Error())
	case errors.Is(err, ErrEventTypeInvalid):
		h.writeError(w, http.StatusBadRequest, ErrEventTypeInvalid.Error())
	case errors.Is(err, ErrCountryRequired):
		h.writeError(w, http.StatusBadRequest, ErrCountryRequired.Error())
	case errors.Is(err, ErrPlatformInvalid):
		h.writeError(w, http.StatusBadRequest, ErrPlatformInvalid.Error())
	case errors.Is(err, ErrPurchaseRevenueInvalid):
		h.writeError(w, http.StatusBadRequest, ErrPurchaseRevenueInvalid.Error())
	case errors.Is(err, ErrTimestampRequired):
		h.writeError(w, http.StatusBadRequest, ErrTimestampRequired.Error())
	case errors.Is(err, ErrAppNotFound):
		h.writeError(w, http.StatusNotFound, ErrAppNotFound.Error())
	default:
		h.logger.Error("event request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	if err := writeJSON(w, status, errorResponse{Error: message}); err != nil {
		h.logger.Error("write event error response", "status", status, "error", err)
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

type ingestEventRequest struct {
	AppID        uuid.UUID `json:"app_id"`
	EventType    Type      `json:"event_type"`
	Country      string    `json:"country"`
	Platform     Platform  `json:"platform"`
	RevenueCents int64     `json:"revenue_cents"`
	Timestamp    time.Time `json:"timestamp"`
}

type errorResponse struct {
	Error string `json:"error"`
}
