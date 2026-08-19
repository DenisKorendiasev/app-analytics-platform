package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestNewConsumerWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	consumer, err := NewConsumer(ctx, Config{
		URL:        "amqp://guest:guest@localhost:5672/",
		Exchange:   "app.events",
		Queue:      "app.events",
		RoutingKey: "app.events",
	})
	if consumer != nil {
		t.Cleanup(func() {
			if err := consumer.Close(); err != nil {
				t.Errorf("close unexpected consumer: %v", err)
			}
		})
		t.Error("NewConsumer() returned a consumer after the connection attempt failed")
	}
	if err == nil {
		t.Fatal("NewConsumer() error = nil, want an error for a canceled context")
	}
}

func TestConsumerReceiveCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer := &Consumer{deliveries: make(chan amqp.Delivery)}

	_, err := consumer.Receive(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive() error = %v, want context.Canceled", err)
	}
}
