package rabbitmq_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisher(t *testing.T) {
	if os.Getenv("RABBITMQ_INTEGRATION_TEST") != "1" {
		t.Skip("set RABBITMQ_INTEGRATION_TEST=1 to run the RabbitMQ integration test")
	}

	applicationConfig, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	testID := uuid.NewString()
	exchange := "app.events.test." + testID
	queue := "app.events.test." + testID
	routingKey := "app.events.test"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	publisher, err := rabbitmq.NewPublisher(ctx, rabbitmq.Config{
		URL:        applicationConfig.RabbitMQ.URL,
		Exchange:   exchange,
		Queue:      queue,
		RoutingKey: routingKey,
	})
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	})

	connection, err := amqp.Dial(applicationConfig.RabbitMQ.URL)
	if err != nil {
		t.Fatalf("connect consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close consumer connection: %v", err)
		}
	})

	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open consumer channel: %v", err)
	}
	t.Cleanup(func() {
		if _, err := channel.QueueDelete(queue, false, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test queue: %v", err)
		}
		if err := channel.ExchangeDelete(exchange, false, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("delete test exchange: %v", err)
		}
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close consumer channel: %v", err)
		}
	})

	deliveries, err := channel.Consume(queue, fmt.Sprintf("publisher-test-%s", testID), false, false, false, false, nil)
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}

	applicationEvent := event.Event{
		EventID:      uuid.MustParse("785fe217-b25a-43bb-afac-aa94a4108959"),
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 0, time.UTC),
	}
	payload, err := json.Marshal(applicationEvent)
	if err != nil {
		t.Fatalf("marshal expected event: %v", err)
	}
	if err := publisher.Publish(ctx, applicationEvent); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	select {
	case delivery, ok := <-deliveries:
		if !ok {
			t.Fatal("delivery channel closed before a message was received")
		}
		if !bytes.Equal(delivery.Body, payload) {
			t.Errorf("message body = %s, want %s", delivery.Body, payload)
		}
		if delivery.ContentType != "application/json" {
			t.Errorf("message content type = %q, want application/json", delivery.ContentType)
		}
		if delivery.DeliveryMode != amqp.Persistent {
			t.Errorf("message delivery mode = %d, want persistent", delivery.DeliveryMode)
		}
		if err := delivery.Ack(false); err != nil {
			t.Errorf("ack message: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for message: %v", ctx.Err())
	}

	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := publisher.Publish(ctx, applicationEvent); err == nil {
		t.Error("Publish() after Close() error = nil, want an error")
	}
}
