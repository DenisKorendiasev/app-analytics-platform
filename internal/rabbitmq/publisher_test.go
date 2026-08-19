package rabbitmq

import (
	"context"
	"testing"
)

func TestNewPublisherWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	publisher, err := NewPublisher(ctx, Config{
		URL:        "amqp://guest:guest@localhost:5672/",
		Exchange:   "app.events",
		Queue:      "app.events",
		RoutingKey: "app.events",
	})
	if publisher != nil {
		t.Cleanup(func() {
			if err := publisher.Close(); err != nil {
				t.Errorf("close unexpected publisher: %v", err)
			}
		})
		t.Error("NewPublisher() returned a publisher after the connection attempt failed")
	}
	if err == nil {
		t.Fatal("NewPublisher() error = nil, want an error for a canceled context")
	}
}
