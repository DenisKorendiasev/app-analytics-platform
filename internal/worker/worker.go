// Package worker contains the event consumer application logic.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
)

// Delivery contains a received Event and its acknowledgement operation.
type Delivery struct {
	Event event.Event
	Ack   func() error
}

// Consumer provides RabbitMQ deliveries to the Worker.
type Consumer interface {
	Receive(ctx context.Context) (Delivery, error)
	Stop() error
}

// EventWriter persists events before their RabbitMQ deliveries are acknowledged.
type EventWriter interface {
	Insert(ctx context.Context, applicationEvent event.Event) error
}

// Worker receives and persists application events.
type Worker struct {
	consumer Consumer
	writer   EventWriter
	logger   *slog.Logger
}

// New creates a Worker.
func New(consumer Consumer, writer EventWriter, logger *slog.Logger) *Worker {
	return &Worker{consumer: consumer, writer: writer, logger: logger}
}

// Run processes deliveries until the context is canceled or consumption fails.
func (w *Worker) Run(ctx context.Context) error {
	for {
		delivery, err := w.consumer.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				if err := w.consumer.Stop(); err != nil {
					return fmt.Errorf("stop event consumer: %w", err)
				}
				return nil
			}
			return fmt.Errorf("receive event: %w", err)
		}

		// Cancellation stops new deliveries, but an event already received must
		// finish persistence before it can be acknowledged or the Worker exits.
		if err := w.writer.Insert(context.WithoutCancel(ctx), delivery.Event); err != nil {
			return fmt.Errorf("persist event %s: %w", delivery.Event.EventID, err)
		}

		w.logger.Info("event persisted",
			"event_id", delivery.Event.EventID,
			"app_id", delivery.Event.AppID,
			"event_type", delivery.Event.EventType,
			"country", delivery.Event.Country,
			"platform", delivery.Event.Platform,
			"revenue_cents", delivery.Event.RevenueCents,
			"timestamp", delivery.Event.Timestamp,
		)
		if err := delivery.Ack(); err != nil {
			return fmt.Errorf("acknowledge event %s: %w", delivery.Event.EventID, err)
		}
	}
}
