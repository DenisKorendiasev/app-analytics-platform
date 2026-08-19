package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	workerapp "github.com/DenisKorendiasev/app-analytics-platform/internal/worker"
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

	analyticsStore, err := clickhouseinfra.Open(signalContext, clickhouseinfra.Config{
		Host:     cfg.ClickHouse.Host,
		Port:     cfg.ClickHouse.Port,
		Database: cfg.ClickHouse.Database,
		User:     cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	})
	if err != nil {
		logger.Error("connect to ClickHouse", "error", err)
		return 1
	}
	defer func() {
		if err := analyticsStore.Close(); err != nil {
			logger.Error("close ClickHouse connection", "error", err)
			return
		}
		logger.Info("ClickHouse connection closed")
	}()
	logger.Info("connected to ClickHouse", "host", cfg.ClickHouse.Host, "port", cfg.ClickHouse.Port)

	consumer, err := rabbitmq.NewConsumer(signalContext, rabbitmq.Config{
		URL:        cfg.RabbitMQ.URL,
		Exchange:   cfg.RabbitMQ.Exchange,
		Queue:      cfg.RabbitMQ.Queue,
		RoutingKey: cfg.RabbitMQ.RoutingKey,
	})
	if err != nil {
		logger.Error("connect RabbitMQ consumer", "error", err)
		return 1
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("close RabbitMQ consumer", "error", err)
			return
		}
		logger.Info("RabbitMQ consumer closed")
	}()

	eventRepository := clickhouseinfra.NewEventRepository(analyticsStore)
	applicationWorker := workerapp.New(consumer, eventRepository, logger)
	logger.Info("Worker started", "queue", cfg.RabbitMQ.Queue)
	if err := applicationWorker.Run(signalContext); err != nil {
		logger.Error("Worker stopped", "error", err)
		return 1
	}
	logger.Info("Worker stopped")
	return 0
}
