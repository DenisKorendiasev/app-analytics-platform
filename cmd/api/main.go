package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/httpserver"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	server := httpserver.New(fmt.Sprintf(":%d", cfg.HTTPPort), logger)

	signalContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverError := make(chan error, 1)
	go func() {
		logger.Info("HTTP server started", "port", cfg.HTTPPort)
		serverError <- server.Start()
	}()

	select {
	case err := <-serverError:
		if err != nil {
			logger.Error("HTTP server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	case <-signalContext.Done():
		logger.Info("shutdown signal received")

		shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownContext); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
		if err := <-serverError; err != nil {
			logger.Error("HTTP server stopped", "error", err)
			os.Exit(1)
		}
		logger.Info("HTTP server stopped")
	}
}
