package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/worker"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWorkerPersistsPublishedEvent(t *testing.T) {
	if os.Getenv("RABBITMQ_INTEGRATION_TEST") != "1" || os.Getenv("CLICKHOUSE_INTEGRATION_TEST") != "1" {
		t.Skip("set RABBITMQ_INTEGRATION_TEST=1 and CLICKHOUSE_INTEGRATION_TEST=1 to run the Worker integration test")
	}

	applicationConfig, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}
	testID := uuid.NewString()
	rabbitConfig := rabbitmq.Config{
		URL:        applicationConfig.RabbitMQ.URL,
		Exchange:   "app.events.worker.test." + testID,
		Queue:      "app.events.worker.test." + testID,
		RoutingKey: "app.events.worker.test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clickHouseConnection := openWorkerTestClickHouse(t, ctx, applicationConfig.ClickHouse)

	consumer, err := rabbitmq.NewConsumer(ctx, rabbitConfig)
	if err != nil {
		t.Fatalf("create RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close RabbitMQ consumer: %v", err)
		}
	})
	publisher, err := rabbitmq.NewPublisher(ctx, rabbitConfig)
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})

	inspectionConnection, err := amqp.Dial(rabbitConfig.URL)
	if err != nil {
		t.Fatalf("connect RabbitMQ inspector: %v", err)
	}
	inspectionChannel, err := inspectionConnection.Channel()
	if err != nil {
		_ = inspectionConnection.Close()
		t.Fatalf("open RabbitMQ inspection channel: %v", err)
	}
	t.Cleanup(func() {
		if _, err := inspectionChannel.QueueDelete(rabbitConfig.Queue, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test queue: %v", err)
		}
		if err := inspectionChannel.ExchangeDelete(rabbitConfig.Exchange, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test exchange: %v", err)
		}
		if err := inspectionChannel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspection channel: %v", err)
		}
		if err := inspectionConnection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspection connection: %v", err)
		}
	})

	records := make(chan slog.Record, 1)
	logger := slog.New(&captureHandler{records: records})
	workerContext, stopWorker := context.WithCancel(ctx)
	workerResult := make(chan error, 1)
	go func() {
		workerResult <- worker.New(consumer, clickhouseinfra.NewEventRepository(clickHouseConnection), logger).Run(workerContext)
	}()

	want := integrationEvent()
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	select {
	case record := <-records:
		assertBatchRecord(t, record, 1)
	case err := <-workerResult:
		t.Fatalf("Worker stopped before persisting the event: %v", err)
	case <-ctx.Done():
		t.Fatalf("wait for Worker persistence: %v", ctx.Err())
	}
	assertPersistedEvent(t, ctx, clickHouseConnection, want)
	stopWorker()
	select {
	case err := <-workerResult:
		if err != nil {
			t.Fatalf("Worker Run() error = %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for Worker shutdown: %v", ctx.Err())
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("close Consumer after Worker shutdown: %v", err)
	}

	queue, err := inspectionChannel.QueueInspect(rabbitConfig.Queue)
	if err != nil {
		t.Fatalf("inspect Worker queue: %v", err)
	}
	if queue.Messages != 0 {
		t.Errorf("queue messages = %d, want 0 after acknowledgement", queue.Messages)
	}
	if queue.Consumers != 0 {
		t.Errorf("queue consumers = %d, want 0 after shutdown", queue.Consumers)
	}
}

func integrationEvent() event.Event {
	return event.Event{
		EventID:      uuid.MustParse("785fe217-b25a-43bb-afac-aa94a4108959"),
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 123000000, time.UTC),
	}
}

func assertBatchRecord(t *testing.T, record slog.Record, wantSize int) {
	t.Helper()

	if record.Message != "event batch processed" {
		t.Errorf("log message = %q, want event batch processed", record.Message)
	}
	attributes := make(map[string]string)
	record.Attrs(func(attribute slog.Attr) bool {
		attributes[attribute.Key] = fmt.Sprint(attribute.Value.Any())
		return true
	})
	if attributes["batch_size"] != fmt.Sprint(wantSize) {
		t.Errorf("batch_size = %q, want %d", attributes["batch_size"], wantSize)
	}
}

type captureHandler struct {
	records chan<- slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "event batch processed" {
		h.records <- record.Clone()
	}
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(string) slog.Handler {
	return h
}

func openWorkerTestClickHouse(t *testing.T, ctx context.Context, cfg config.ClickHouseConfig) driver.Conn {
	t.Helper()

	adminConfig := workerClickHouseConfig(cfg)
	adminConfig.Database = "default"
	adminConnection, err := clickhouseinfra.Open(ctx, adminConfig)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() {
		if err := adminConnection.Close(); err != nil {
			t.Errorf("close ClickHouse admin connection: %v", err)
		}
	})

	databaseName := "worker_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := adminConnection.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create ClickHouse test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := adminConnection.Exec(cleanupContext, "DROP DATABASE IF EXISTS "+databaseName+" SYNC"); err != nil {
			t.Errorf("drop ClickHouse test database: %v", err)
		}
	})

	testConfig := workerClickHouseConfig(cfg)
	testConfig.Database = databaseName
	connection, err := clickhouseinfra.Open(ctx, testConfig)
	if err != nil {
		t.Fatalf("open ClickHouse test connection: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close ClickHouse test connection: %v", err)
		}
	})

	migration, err := os.ReadFile("../../migrations/clickhouse/000001_create_events.up.sql")
	if err != nil {
		t.Fatalf("read ClickHouse migration: %v", err)
	}
	if err := connection.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply ClickHouse migration: %v", err)
	}
	return connection
}

func workerClickHouseConfig(cfg config.ClickHouseConfig) clickhouseinfra.Config {
	return clickhouseinfra.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Database,
		User:     cfg.User,
		Password: cfg.Password,
	}
}

func assertPersistedEvent(t *testing.T, ctx context.Context, connection driver.Conn, want event.Event) {
	t.Helper()

	const query = `
		SELECT event_id, app_id, event_type, country, platform, revenue_cents, timestamp
		FROM events
		WHERE event_id = ?`
	var (
		gotEventID      uuid.UUID
		gotAppID        uuid.UUID
		gotEventType    string
		gotCountry      string
		gotPlatform     string
		gotRevenueCents int64
		gotTimestamp    time.Time
	)
	if err := connection.QueryRow(ctx, query, want.EventID).Scan(
		&gotEventID,
		&gotAppID,
		&gotEventType,
		&gotCountry,
		&gotPlatform,
		&gotRevenueCents,
		&gotTimestamp,
	); err != nil {
		t.Fatalf("select persisted event: %v", err)
	}
	got := event.Event{
		EventID:      gotEventID,
		AppID:        gotAppID,
		EventType:    event.Type(gotEventType),
		Country:      gotCountry,
		Platform:     event.Platform(gotPlatform),
		RevenueCents: gotRevenueCents,
		Timestamp:    gotTimestamp.UTC(),
	}
	if got != want {
		t.Errorf("persisted event = %+v, want %+v", got, want)
	}
}
