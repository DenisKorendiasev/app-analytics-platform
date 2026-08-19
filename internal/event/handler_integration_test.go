package event_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/app"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestEventIngestion(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION_TEST") != "1" || os.Getenv("RABBITMQ_INTEGRATION_TEST") != "1" {
		t.Skip("set POSTGRES_INTEGRATION_TEST=1 and RABBITMQ_INTEGRATION_TEST=1 to run the event ingestion integration test")
	}

	applicationConfig, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := openTestPool(t, ctx, applicationConfig.Postgres)
	appRepository := postgres.NewAppRepository(pool)
	application, err := app.NewService(appRepository).Create(ctx, "Event Test App", "Test Publisher", "test")
	if err != nil {
		t.Fatalf("create application: %v", err)
	}

	testID := uuid.NewString()
	exchange := "app.events.ingestion.test." + testID
	queue := "app.events.ingestion.test." + testID
	publisher, err := rabbitmq.NewPublisher(ctx, rabbitmq.Config{
		URL:        applicationConfig.RabbitMQ.URL,
		Exchange:   exchange,
		Queue:      queue,
		RoutingKey: "app.events.ingestion.test",
	})
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})

	connection, err := amqp.Dial(applicationConfig.RabbitMQ.URL)
	if err != nil {
		t.Fatalf("connect RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ consumer connection: %v", err)
		}
	})
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ consumer channel: %v", err)
	}
	t.Cleanup(func() {
		if _, err := channel.QueueDelete(queue, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test queue: %v", err)
		}
		if err := channel.ExchangeDelete(exchange, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test exchange: %v", err)
		}
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ consumer channel: %v", err)
		}
	})
	deliveries, err := channel.Consume(queue, "event-ingestion-test-"+testID, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("start RabbitMQ consumer: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := event.NewHandler(event.NewService(appRepository, publisher), logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	timestamp := time.Date(2026, time.August, 18, 12, 35, 2, 0, time.UTC)
	requestBody := fmt.Sprintf(
		`{"app_id":%q,"event_type":"purchase","country":"RS","platform":"android","revenue_cents":999,"timestamp":%q}`,
		application.ID,
		timestamp.Format(time.RFC3339),
	)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(requestBody)).WithContext(ctx)
	response := httptest.NewRecorder()

	mux.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, http.StatusAccepted, response.Body)
	}

	select {
	case delivery, ok := <-deliveries:
		if !ok {
			t.Fatal("delivery channel closed before an event was received")
		}
		var applicationEvent event.Event
		if err := json.Unmarshal(delivery.Body, &applicationEvent); err != nil {
			t.Fatalf("decode published event: %v", err)
		}
		if applicationEvent.EventID == uuid.Nil {
			t.Error("published event ID is empty")
		}
		if applicationEvent.AppID != application.ID || applicationEvent.EventType != event.TypePurchase || applicationEvent.Country != "RS" || applicationEvent.Platform != event.PlatformAndroid || applicationEvent.RevenueCents != 999 || !applicationEvent.Timestamp.Equal(timestamp) {
			t.Errorf("published event = %+v, want request fields for app %s", applicationEvent, application.ID)
		}
		if delivery.ContentType != "application/json" {
			t.Errorf("message content type = %q, want application/json", delivery.ContentType)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Errorf("message delivery mode = %d, want persistent", delivery.DeliveryMode)
		}
		if err := delivery.Ack(false); err != nil {
			t.Errorf("ack event: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for published event: %v", ctx.Err())
	}
}

func openTestPool(t *testing.T, ctx context.Context, cfg config.PostgresConfig) *pgxpool.Pool {
	t.Helper()

	adminPool, err := postgres.Open(ctx, postgres.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Database,
		User:     cfg.User,
		Password: cfg.Password,
		SSLMode:  cfg.SSLMode,
	})
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := fmt.Sprintf("event_ingestion_test_%x", uuid.New())
	schemaIdentifier := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := adminPool.Exec(cleanupContext, "DROP SCHEMA "+schemaIdentifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	poolConfig := adminPool.Config()
	runtimeParams := make(map[string]string, len(poolConfig.ConnConfig.RuntimeParams)+1)
	for key, value := range poolConfig.ConnConfig.RuntimeParams {
		runtimeParams[key] = value
	}
	runtimeParams["search_path"] = schemaName
	poolConfig.ConnConfig.RuntimeParams = runtimeParams
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create PostgreSQL test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping PostgreSQL test pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migration, err := os.ReadFile("../../migrations/postgres/000001_create_apps.up.sql")
	if err != nil {
		t.Fatalf("read apps migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply apps migration: %v", err)
	}
	return pool
}
