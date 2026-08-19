//go:build integration

package integration_test

import (
	"bytes"
	"context"
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
		if delivery.MessageId != want.EventID.String() {
			t.Errorf("message ID = %q, want %q", delivery.MessageId, want.EventID)
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

func TestRabbitMQConsumerDeadLettersPoisonMessage(t *testing.T) {
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

	connection, channel := openRabbitInspector(t, configuration.URL)
	t.Cleanup(func() {
		if err := channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspector channel: %v", err)
		}
		if err := connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("close RabbitMQ inspector connection: %v", err)
		}
	})

	poisonBody := []byte(`{"event_id":`)
	if err := publishRaw(ctx, channel, configuration, poisonBody); err != nil {
		t.Fatalf("publish poison message: %v", err)
	}
	want := integrationEvent(uuid.MustParse("d12f3583-65a7-477a-a664-3e04f93eaf43"))
	wantBody, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal valid event: %v", err)
	}
	if err := publishRaw(ctx, channel, configuration, wantBody); err != nil {
		t.Fatalf("publish valid message: %v", err)
	}

	delivery, err := consumer.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if delivery.Event != want {
		t.Errorf("Receive() event = %+v, want valid event %+v", delivery.Event, want)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	deadLetter := waitForRabbitMessage(t, ctx, channel, rabbitmq.DeadLetterQueueName(configuration))
	if !bytes.Equal(deadLetter.Body, poisonBody) {
		t.Errorf("dead-letter body = %s, want %s", deadLetter.Body, poisonBody)
	}
	if reason, ok := deadLetter.Headers["x-first-death-reason"].(string); !ok || reason != "rejected" {
		t.Errorf("x-first-death-reason = %#v, want rejected", deadLetter.Headers["x-first-death-reason"])
	}
}

func TestRabbitMQUnacknowledgedDeliveryIsRedelivered(t *testing.T) {
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

	want := integrationEvent(uuid.MustParse("780c5ed4-f22a-4fa7-b0cd-e096b2d3892f"))
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	firstConsumer, err := rabbitmq.NewConsumer(ctx, configuration)
	if err != nil {
		t.Fatalf("create first RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := firstConsumer.Close(); err != nil {
			t.Errorf("close first RabbitMQ consumer: %v", err)
		}
	})
	firstDelivery, err := firstConsumer.Receive(ctx)
	if err != nil {
		t.Fatalf("receive first delivery: %v", err)
	}
	if firstDelivery.Event.EventID != want.EventID {
		t.Fatalf("first event ID = %s, want %s", firstDelivery.Event.EventID, want.EventID)
	}
	if err := firstConsumer.Close(); err != nil {
		t.Fatalf("close first consumer without acknowledgement: %v", err)
	}

	secondConsumer, err := rabbitmq.NewConsumer(ctx, configuration)
	if err != nil {
		t.Fatalf("create second RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := secondConsumer.Close(); err != nil {
			t.Errorf("close second RabbitMQ consumer: %v", err)
		}
	})
	redelivery, err := secondConsumer.Receive(ctx)
	if err != nil {
		t.Fatalf("receive redelivery: %v", err)
	}
	if redelivery.Event.EventID != want.EventID {
		t.Errorf("redelivered event ID = %s, want %s", redelivery.Event.EventID, want.EventID)
	}
	if err := redelivery.Ack(); err != nil {
		t.Fatalf("ack redelivery: %v", err)
	}
}

func openRabbitInspector(t *testing.T, connectionURL string) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	connection, err := amqp.Dial(connectionURL)
	if err != nil {
		t.Fatalf("connect RabbitMQ inspector: %v", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		t.Fatalf("open RabbitMQ inspector channel: %v", err)
	}
	return connection, channel
}

func publishRaw(ctx context.Context, channel *amqp.Channel, configuration rabbitmq.Config, body []byte) error {
	return channel.PublishWithContext(ctx, configuration.Exchange, configuration.RoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func waitForRabbitMessage(t *testing.T, ctx context.Context, channel *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		delivery, ok, err := channel.Get(queue, true)
		if err != nil {
			t.Fatalf("get RabbitMQ message from %q: %v", queue, err)
		}
		if ok {
			return delivery
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for RabbitMQ message from %q: %v", queue, ctx.Err())
		case <-ticker.C:
		}
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
