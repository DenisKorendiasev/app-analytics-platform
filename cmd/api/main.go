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
	"github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load configuration", "error", err)
		return 1
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	signalContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database, err := postgres.Open(signalContext, postgres.Config{
		Host:     cfg.Postgres.Host,
		Port:     cfg.Postgres.Port,
		Database: cfg.Postgres.Database,
		User:     cfg.Postgres.User,
		Password: cfg.Postgres.Password,
		SSLMode:  cfg.Postgres.SSLMode,
	})
	if err != nil {
		logger.Error("connect to PostgreSQL", "error", err)
		return 1
	}
	defer func() {
		database.Close()
		logger.Info("PostgreSQL pool closed")
	}()
	logger.Info("connected to PostgreSQL", "host", cfg.Postgres.Host, "port", cfg.Postgres.Port)

	server := httpserver.New(fmt.Sprintf(":%d", cfg.HTTPPort), logger)
	serverError := make(chan error, 1)
	go func() {
		logger.Info("HTTP server started", "port", cfg.HTTPPort)
		serverError <- server.Start()
	}()

	select {
	case err := <-serverError:
		if err != nil {
			logger.Error("HTTP server stopped unexpectedly", "error", err)
			return 1
		}
	case <-signalContext.Done():
		logger.Info("shutdown signal received")

		shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownContext); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			return 1
		}
		if err := <-serverError; err != nil {
			logger.Error("HTTP server stopped", "error", err)
			return 1
		}
		logger.Info("HTTP server stopped")
	}

	return 0
}
