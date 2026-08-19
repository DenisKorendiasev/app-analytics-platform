// Package worker contains the event consumer application logic.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
)

const (
	// BatchSize is the number of deliveries that triggers an immediate flush.
	BatchSize = 500
	// BatchTimeout is the maximum wait after the first delivery in a batch.
	BatchTimeout = time.Second
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

// EventWriter persists event batches before their RabbitMQ deliveries are acknowledged.
type EventWriter interface {
	InsertBatch(ctx context.Context, events []event.Event) error
}

// Worker receives and persists application events in batches.
type Worker struct {
	consumer     Consumer
	writer       EventWriter
	logger       *slog.Logger
	batchSize    int
	batchTimeout time.Duration
}

// New creates a Worker with the production batch size and timeout.
func New(consumer Consumer, writer EventWriter, logger *slog.Logger) *Worker {
	return newWorker(consumer, writer, logger, BatchSize, BatchTimeout)
}

func newWorker(consumer Consumer, writer EventWriter, logger *slog.Logger, batchSize int, batchTimeout time.Duration) *Worker {
	return &Worker{
		consumer:     consumer,
		writer:       writer,
		logger:       logger,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
	}
}

// Run processes deliveries until consumption fails or graceful shutdown completes.
func (w *Worker) Run(ctx context.Context) error {
	receiveContext, cancelReceive := context.WithCancel(context.WithoutCancel(ctx))
	deliveries := make(chan Delivery, w.batchSize)
	receiveResult := make(chan error, 1)
	go w.receive(receiveContext, deliveries, receiveResult)

	receiverDone := false
	defer func() {
		if receiverDone {
			return
		}
		cancelReceive()
		for range deliveries {
		}
		<-receiveResult
	}()

	timer := time.NewTimer(w.batchTimeout)
	stopTimer(timer)
	defer stopTimer(timer)
	var timerChannel <-chan time.Time
	batch := make([]Delivery, 0, w.batchSize)

	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				receiveError := <-receiveResult
				receiverDone = true
				if ctx.Err() != nil {
					return errors.Join(
						stopConsumer(w.consumer),
						w.flush(context.WithoutCancel(ctx), batch),
					)
				}
				return fmt.Errorf("receive event: %w", receiveError)
			}

			batch = append(batch, delivery)
			if len(batch) == 1 {
				resetTimer(timer, w.batchTimeout)
				timerChannel = timer.C
			}
			if len(batch) >= w.batchSize {
				stopTimer(timer)
				timerChannel = nil
				if err := w.flush(context.WithoutCancel(ctx), batch); err != nil {
					return err
				}
				clear(batch)
				batch = batch[:0]
			}

		case <-timerChannel:
			timerChannel = nil
			if err := w.flush(context.WithoutCancel(ctx), batch); err != nil {
				return err
			}
			clear(batch)
			batch = batch[:0]

		case <-ctx.Done():
			stopError := stopConsumer(w.consumer)
			cancelReceive()
			for delivery := range deliveries {
				batch = append(batch, delivery)
			}
			<-receiveResult
			receiverDone = true
			stopTimer(timer)
			timerChannel = nil
			return errors.Join(
				stopError,
				w.flush(context.WithoutCancel(ctx), batch),
			)
		}
	}
}

func (w *Worker) receive(ctx context.Context, deliveries chan<- Delivery, result chan<- error) {
	defer close(deliveries)
	for {
		delivery, err := w.consumer.Receive(ctx)
		if err != nil {
			result <- err
			return
		}

		// Prefer preserving a delivery already returned by the consumer, even
		// when shutdown starts concurrently. The buffered channel is sized to
		// the RabbitMQ prefetch and therefore has room for all in-flight work.
		select {
		case deliveries <- delivery:
			continue
		default:
		}
		select {
		case deliveries <- delivery:
		case <-ctx.Done():
			result <- ctx.Err()
			return
		}
	}
}

func (w *Worker) flush(ctx context.Context, batch []Delivery) error {
	if len(batch) == 0 {
		return nil
	}

	events := make([]event.Event, len(batch))
	for index, delivery := range batch {
		events[index] = delivery.Event
	}
	if err := w.writer.InsertBatch(ctx, events); err != nil {
		return fmt.Errorf("persist event batch of %d: %w", len(batch), err)
	}

	var acknowledgementError error
	for _, delivery := range batch {
		if err := delivery.Ack(); err != nil {
			acknowledgementError = errors.Join(
				acknowledgementError,
				fmt.Errorf("acknowledge event %s: %w", delivery.Event.EventID, err),
			)
		}
	}
	if acknowledgementError != nil {
		return acknowledgementError
	}

	w.logger.Info("event batch processed", "batch_size", len(batch))
	return nil
}

func stopConsumer(consumer Consumer) error {
	if err := consumer.Stop(); err != nil {
		return fmt.Errorf("stop event consumer: %w", err)
	}
	return nil
}

func resetTimer(timer *time.Timer, timeout time.Duration) {
	stopTimer(timer)
	timer.Reset(timeout)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
