//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQPublishAndMessageFormat(t *testing.T) {
	ctx := integrationContext(t)
	configuration := newRabbitConfig()
	publisher, err := rabbitmq.NewPublisher(ctx, configuration)
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})

	connection, err := amqp.Dial(configuration.URL)
	if err != nil {
		t.Fatalf("connect RabbitMQ inspector: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspector connection: %v", err)
		}
	})
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ inspector channel: %v", err)
	}
	t.Cleanup(func() {
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspector channel: %v", err)
		}
	})
	deliveries, err := channel.Consume(configuration.Queue, "message-format-test", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("start raw RabbitMQ consumer: %v", err)
	}

	want := integrationEvent(uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"))
	wantPayload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal expected event: %v", err)
	}
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case delivery, ok := <-deliveries:
		if !ok {
			t.Fatal("RabbitMQ delivery channel closed before receiving a message")
		}
		if !bytes.Equal(delivery.Body, wantPayload) {
			t.Errorf("message body = %s, want %s", delivery.Body, wantPayload)
		}
		if delivery.ContentType != "application/json" {
			t.Errorf("content type = %q, want application/json", delivery.ContentType)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Errorf("delivery mode = %d, want persistent", delivery.DeliveryMode)
		}
		if err := delivery.Ack(false); err != nil {
			t.Errorf("ack raw RabbitMQ delivery: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for RabbitMQ message: %v", ctx.Err())
	}
}

func TestRabbitMQConsumer(t *testing.T) {
	ctx := integrationContext(t)
	configuration := newRabbitConfig()
	consumer, err := rabbitmq.NewConsumer(ctx, configuration)
	if err != nil {
		t.Fatalf("create RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close RabbitMQ consumer: %v", err)
		}
	})
	publisher, err := rabbitmq.NewPublisher(ctx, configuration)
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})

	want := integrationEvent(uuid.MustParse("d12f3583-65a7-477a-a664-3e04f93eaf43"))
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	delivery, err := consumer.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if delivery.Event != want {
		t.Errorf("Receive() event = %+v, want %+v", delivery.Event, want)
	}
	if err := delivery.Ack(); err != nil {
		t.Errorf("Ack() error = %v", err)
	}
}

func integrationEvent(appID uuid.UUID) event.Event {
	return event.Event{
		EventID:      uuid.New(),
		AppID:        appID,
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 123000000, time.UTC),
	}
}
