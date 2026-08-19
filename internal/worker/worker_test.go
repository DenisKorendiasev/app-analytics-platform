package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestWorkerProductionBatchSettings(t *testing.T) {
	if BatchSize != 500 {
		t.Errorf("BatchSize = %d, want 500", BatchSize)
	}
	if BatchTimeout != time.Second {
		t.Errorf("BatchTimeout = %s, want 1s", BatchTimeout)
	}
	if MaxInsertAttempts != 3 {
		t.Errorf("MaxInsertAttempts = %d, want 3", MaxInsertAttempts)
	}
	if InsertRetryBackoff != 100*time.Millisecond {
		t.Errorf("InsertRetryBackoff = %s, want 100ms", InsertRetryBackoff)
	}
}

func TestWorkerFlushesImmediatelyAtBatchSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acknowledged atomic.Int64
	deliveries := testDeliveries(BatchSize, func() error {
		acknowledged.Add(1)
		return nil
	})
	consumer := newSequenceConsumer(deliveries, nil)
	var logs bytes.Buffer
	writer := &writerStub{insertBatch: func(processingContext context.Context, events []event.Event) error {
		if err := processingContext.Err(); err != nil {
			t.Errorf("processing context error = %v", err)
		}
		if len(events) != BatchSize {
			t.Errorf("batch size = %d, want %d", len(events), BatchSize)
		}
		cancel()
		return nil
	}}

	err := New(consumer, writer, slog.New(slog.NewJSONHandler(&logs, nil))).Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if writer.callCount() != 1 {
		t.Errorf("InsertBatch() calls = %d, want 1", writer.callCount())
	}
	if acknowledged.Load() != BatchSize {
		t.Errorf("acknowledged deliveries = %d, want %d", acknowledged.Load(), BatchSize)
	}
	if consumer.stopCount() != 1 {
		t.Errorf("Stop() calls = %d, want 1", consumer.stopCount())
	}
	for _, value := range []string{"event batch processed", `"batch_size":500`} {
		if !strings.Contains(logs.String(), value) {
			t.Errorf("log output does not contain %q: %s", value, logs.String())
		}
	}
}

func TestWorkerFlushesPartialBatchOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const eventCount = 100
	var acknowledged atomic.Int64
	consumer := newSequenceConsumer(testDeliveries(eventCount, func() error {
		acknowledged.Add(1)
		return nil
	}), nil)
	writer := &writerStub{insertBatch: func(_ context.Context, events []event.Event) error {
		if len(events) != eventCount {
			t.Errorf("batch size = %d, want %d", len(events), eventCount)
		}
		cancel()
		return nil
	}}

	err := newWorker(consumer, writer, discardLogger(), BatchSize, 10*time.Millisecond).Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if writer.callCount() != 1 {
		t.Errorf("InsertBatch() calls = %d, want 1", writer.callCount())
	}
	if acknowledged.Load() != eventCount {
		t.Errorf("acknowledged deliveries = %d, want %d", acknowledged.Load(), eventCount)
	}
}

func TestWorkerExhaustedPersistenceRetriesDeadLetterBatch(t *testing.T) {
	persistError := errors.New("ClickHouse unavailable")
	rejectError := errors.New("reject failed")
	var acknowledged atomic.Int64
	var rejected atomic.Int64
	deliveries := testDeliveries(2, func() error {
		acknowledged.Add(1)
		return nil
	})
	for index := range deliveries {
		deliveries[index].Reject = func() error {
			if rejected.Add(1) == 1 {
				return rejectError
			}
			return nil
		}
	}
	consumer := newSequenceConsumer(deliveries, nil)
	writer := &writerStub{insertBatch: func(context.Context, []event.Event) error {
		return persistError
	}}

	err := newWorkerWithRetry(consumer, writer, discardLogger(), 2, time.Hour, 3, 0).Run(context.Background())
	if !errors.Is(err, persistError) {
		t.Fatalf("Run() error = %v, want wrapped persistence error", err)
	}
	if !errors.Is(err, rejectError) {
		t.Fatalf("Run() error = %v, want wrapped rejection error", err)
	}
	if writer.callCount() != 3 {
		t.Errorf("InsertBatch() calls = %d, want 3", writer.callCount())
	}
	if acknowledged.Load() != 0 {
		t.Errorf("acknowledged deliveries = %d, want 0", acknowledged.Load())
	}
	if rejected.Load() != 2 {
		t.Errorf("rejected deliveries = %d, want 2", rejected.Load())
	}
}

func TestWorkerRetriesTransientPersistenceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acknowledged atomic.Int64
	var rejected atomic.Int64
	deliveries := testDeliveries(2, func() error {
		acknowledged.Add(1)
		return nil
	})
	for index := range deliveries {
		deliveries[index].Reject = func() error {
			rejected.Add(1)
			return nil
		}
	}
	consumer := newSequenceConsumer(deliveries, nil)
	transientError := errors.New("temporary ClickHouse error")
	var attempts atomic.Int64
	writer := &writerStub{insertBatch: func(context.Context, []event.Event) error {
		if attempts.Add(1) < 3 {
			return transientError
		}
		cancel()
		return nil
	}}

	err := newWorkerWithRetry(consumer, writer, discardLogger(), 2, time.Hour, 3, 0).Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("InsertBatch() attempts = %d, want 3", attempts.Load())
	}
	if acknowledged.Load() != 2 {
		t.Errorf("acknowledged deliveries = %d, want 2", acknowledged.Load())
	}
	if rejected.Load() != 0 {
		t.Errorf("rejected deliveries = %d, want 0", rejected.Load())
	}
}

func TestWorkerDeduplicatesEventIDsWithinBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acknowledged atomic.Int64
	deliveries := testDeliveries(2, func() error {
		acknowledged.Add(1)
		return nil
	})
	deliveries[1].Event = deliveries[0].Event
	consumer := newSequenceConsumer(deliveries, nil)
	writer := &writerStub{insertBatch: func(_ context.Context, events []event.Event) error {
		if len(events) != 1 {
			t.Errorf("unique event count = %d, want 1", len(events))
		}
		cancel()
		return nil
	}}

	if err := newWorkerWithRetry(consumer, writer, discardLogger(), 2, time.Hour, 3, 0).Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if acknowledged.Load() != 2 {
		t.Errorf("acknowledged deliveries = %d, want 2", acknowledged.Load())
	}
}

func TestWorkerShutdownFlushesRemainingBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	const eventCount = 100
	var acknowledged atomic.Int64
	consumer := newSequenceConsumer(testDeliveries(eventCount, func() error {
		acknowledged.Add(1)
		return nil
	}), cancel)
	writer := &writerStub{insertBatch: func(processingContext context.Context, events []event.Event) error {
		if err := processingContext.Err(); err != nil {
			t.Errorf("shutdown processing context error = %v", err)
		}
		if len(events) != eventCount {
			t.Errorf("shutdown batch size = %d, want %d", len(events), eventCount)
		}
		return nil
	}}

	err := newWorker(consumer, writer, discardLogger(), BatchSize, time.Hour).Run(ctx)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if writer.callCount() != 1 {
		t.Errorf("InsertBatch() calls = %d, want 1", writer.callCount())
	}
	if acknowledged.Load() != eventCount {
		t.Errorf("acknowledged deliveries = %d, want %d", acknowledged.Load(), eventCount)
	}
	if consumer.stopCount() != 1 {
		t.Errorf("Stop() calls = %d, want 1", consumer.stopCount())
	}
}

func TestWorkerAttemptsAllAcknowledgements(t *testing.T) {
	ackError := errors.New("ack failed")
	var acknowledgementCalls atomic.Int64
	deliveries := testDeliveries(2, func() error {
		call := acknowledgementCalls.Add(1)
		if call == 1 {
			return ackError
		}
		return nil
	})
	consumer := newSequenceConsumer(deliveries, nil)

	err := newWorker(consumer, &writerStub{}, discardLogger(), 2, time.Hour).Run(context.Background())
	if !errors.Is(err, ackError) {
		t.Fatalf("Run() error = %v, want wrapped acknowledgement error", err)
	}
	if acknowledgementCalls.Load() != 2 {
		t.Errorf("acknowledgement calls = %d, want 2", acknowledgementCalls.Load())
	}
}

func TestWorkerReceiveError(t *testing.T) {
	receiveError := errors.New("consumer unavailable")
	consumer := &consumerStub{receive: func(context.Context) (Delivery, error) {
		return Delivery{}, receiveError
	}}

	err := New(consumer, &writerStub{}, discardLogger()).Run(context.Background())
	if !errors.Is(err, receiveError) {
		t.Fatalf("Run() error = %v, want wrapped receive error", err)
	}
	if consumer.stopCount() != 0 {
		t.Errorf("Stop() calls = %d, want 0", consumer.stopCount())
	}
}

func TestWorkerStopError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopError := errors.New("cancel consumer failed")
	consumer := &consumerStub{
		receive: func(ctx context.Context) (Delivery, error) {
			<-ctx.Done()
			return Delivery{}, ctx.Err()
		},
		stop: func() error { return stopError },
	}

	err := New(consumer, &writerStub{}, discardLogger()).Run(ctx)
	if !errors.Is(err, stopError) {
		t.Fatalf("Run() error = %v, want wrapped stop error", err)
	}
}

func testDeliveries(count int, ack func() error) []Delivery {
	deliveries := make([]Delivery, count)
	for index := range deliveries {
		deliveries[index] = Delivery{
			Event:  testEvent(index),
			Ack:    ack,
			Reject: func() error { return nil },
		}
	}
	return deliveries
}

func testEvent(index int) event.Event {
	return event.Event{
		EventID:      uuid.New(),
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: int64(index),
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, index*1000000, time.UTC),
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type sequenceConsumer struct {
	mu             sync.Mutex
	deliveries     []Delivery
	next           int
	stopCalls      int
	afterLast      func()
	afterLastFired bool
}

func newSequenceConsumer(deliveries []Delivery, afterLast func()) *sequenceConsumer {
	return &sequenceConsumer{deliveries: deliveries, afterLast: afterLast}
}

func (c *sequenceConsumer) Receive(ctx context.Context) (Delivery, error) {
	c.mu.Lock()
	if c.next < len(c.deliveries) {
		delivery := c.deliveries[c.next]
		c.next++
		fire := c.next == len(c.deliveries) && !c.afterLastFired && c.afterLast != nil
		if fire {
			c.afterLastFired = true
		}
		c.mu.Unlock()
		if fire {
			c.afterLast()
		}
		return delivery, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return Delivery{}, ctx.Err()
}

func (c *sequenceConsumer) Stop() error {
	c.mu.Lock()
	c.stopCalls++
	c.mu.Unlock()
	return nil
}

func (c *sequenceConsumer) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCalls
}

type consumerStub struct {
	receive func(context.Context) (Delivery, error)
	stop    func() error

	mu        sync.Mutex
	stopCalls int
}

func (c *consumerStub) Receive(ctx context.Context) (Delivery, error) {
	return c.receive(ctx)
}

func (c *consumerStub) Stop() error {
	c.mu.Lock()
	c.stopCalls++
	c.mu.Unlock()
	if c.stop == nil {
		return nil
	}
	return c.stop()
}

func (c *consumerStub) stopCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCalls
}

type writerStub struct {
	insertBatch func(context.Context, []event.Event) error
	calls       atomic.Int64
}

func (w *writerStub) InsertBatch(ctx context.Context, events []event.Event) error {
	w.calls.Add(1)
	if w.insertBatch == nil {
		return nil
	}
	return w.insertBatch(ctx, events)
}

func (w *writerStub) callCount() int64 {
	return w.calls.Load()
}
