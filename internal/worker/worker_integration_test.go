package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/worker"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestWorkerReceivesPublishedEvent(t *testing.T) {
	if os.Getenv("RABBITMQ_INTEGRATION_TEST") != "1" {
		t.Skip("set RABBITMQ_INTEGRATION_TEST=1 to run the Worker integration test")
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
		workerResult <- worker.New(consumer, logger).Run(workerContext)
	}()

	want := integrationEvent()
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	select {
	case record := <-records:
		assertEventRecord(t, record, want)
	case <-ctx.Done():
		t.Fatalf("wait for Worker log: %v", ctx.Err())
	}
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
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 0, time.UTC),
	}
}

func assertEventRecord(t *testing.T, record slog.Record, want event.Event) {
	t.Helper()

	if record.Message != "event received" {
		t.Errorf("log message = %q, want event received", record.Message)
	}
	attributes := make(map[string]string)
	record.Attrs(func(attribute slog.Attr) bool {
		attributes[attribute.Key] = fmt.Sprint(attribute.Value.Any())
		return true
	})
	wantAttributes := map[string]string{
		"event_id":      want.EventID.String(),
		"app_id":        want.AppID.String(),
		"event_type":    string(want.EventType),
		"country":       want.Country,
		"platform":      string(want.Platform),
		"revenue_cents": fmt.Sprint(want.RevenueCents),
		"timestamp":     want.Timestamp.String(),
	}
	for key, value := range wantAttributes {
		if attributes[key] != value {
			t.Errorf("log attribute %q = %q, want %q", key, attributes[key], value)
		}
	}
}

type captureHandler struct {
	records chan<- slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(_ context.Context, record slog.Record) error {
	if record.Message == "event received" {
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
