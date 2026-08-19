package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
)

const maxRequestBodySize = 1 << 20

// HandlerService defines App operations used by the HTTP adapter.
type HandlerService interface {
	Create(ctx context.Context, name, publisher, category string) (App, error)
	GetByID(ctx context.Context, id uuid.UUID) (App, error)
}

var _ HandlerService = (*Service)(nil)

// Handler exposes App operations over HTTP.
type Handler struct {
	service HandlerService
	logger  *slog.Logger
}

// NewHandler creates an App HTTP handler.
func NewHandler(service HandlerService, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger}
}

// RegisterRoutes registers App HTTP routes on a server mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/apps", h.create)
	mux.HandleFunc("GET /api/v1/apps/{id}", h.getByID)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var request createAppRequest
	if err := decodeJSON(w, r, &request); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	application, err := h.service.Create(r.Context(), request.Name, request.Publisher, request.Category)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	w.Header().Set("Location", "/api/v1/apps/"+application.ID.String())
	if err := writeJSON(w, http.StatusCreated, newAppResponse(application)); err != nil {
		h.logger.Error("write create app response", "error", err)
	}
}

func (h *Handler) getByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid app id")
		return
	}

	application, err := h.service.GetByID(r.Context(), id)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	if err := writeJSON(w, http.StatusOK, newAppResponse(application)); err != nil {
		h.logger.Error("write get app response", "app_id", id, "error", err)
	}
}

func (h *Handler) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNameRequired):
		h.writeError(w, http.StatusBadRequest, ErrNameRequired.Error())
	case errors.Is(err, ErrPublisherRequired):
		h.writeError(w, http.StatusBadRequest, ErrPublisherRequired.Error())
	case errors.Is(err, ErrCategoryRequired):
		h.writeError(w, http.StatusBadRequest, ErrCategoryRequired.Error())
	case errors.Is(err, ErrNotFound):
		h.writeError(w, http.StatusNotFound, ErrNotFound.Error())
	default:
		h.logger.Error("app request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		h.writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	if err := writeJSON(w, status, errorResponse{Error: message}); err != nil {
		h.logger.Error("write error response", "status", status, "error", err)
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
