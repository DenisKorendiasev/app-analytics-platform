package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

const readHeaderTimeout = 5 * time.Second

// Server owns the API HTTP server lifecycle.
type Server struct {
	httpServer *http.Server
}

// New creates an HTTP server with all routes registered.
func New(address string, logger *slog.Logger) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:              address,
			Handler:           newHandler(logger),
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}
}

// Start listens for HTTP requests until the server is shut down.
func (s *Server) Start() error {
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting requests and waits for active requests to finish.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func newHandler(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("{\"status\":\"ok\"}\n")); err != nil {
			logger.Error("write health response", "error", err)
		}
	})
	return mux
}
