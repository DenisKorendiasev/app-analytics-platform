package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestWorkerRunProcessesAndAcknowledgesEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	applicationEvent := testEvent()
	persisted := false
	acknowledged := false
	receiveCalls := 0
	consumer := &consumerStub{
		receive: func(ctx context.Context) (Delivery, error) {
			receiveCalls++
			if receiveCalls == 1 {
				return Delivery{
					Event: applicationEvent,
					Ack: func() error {
						if !persisted {
							t.Error("delivery acknowledged before persistence")
						}
						acknowledged = true
						cancel()
						return nil
					},
				}, nil
			}
			<-ctx.Done()
			return Delivery{}, ctx.Err()
		},
	}
	writer := &writerStub{insert: func(_ context.Context, got event.Event) error {
		if got != applicationEvent {
			t.Errorf("Insert() event = %+v, want %+v", got, applicationEvent)
		}
		persisted = true
		return nil
	}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	if err := New(consumer, writer, logger).Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if !persisted {
		t.Error("event was not persisted")
	}
	if !acknowledged {
		t.Error("delivery was not acknowledged")
	}
	if consumer.stopCalls != 1 {
		t.Errorf("Stop() calls = %d, want 1", consumer.stopCalls)
	}
	for _, value := range []string{"event persisted", applicationEvent.EventID.String(), applicationEvent.AppID.String(), string(applicationEvent.EventType), applicationEvent.Country, string(applicationEvent.Platform)} {
		if !strings.Contains(logs.String(), value) {
			t.Errorf("log output does not contain %q: %s", value, logs.String())
		}
	}
}

func TestWorkerRunFinishesCurrentDeliveryBeforeStopping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	persistStarted := make(chan struct{})
	releasePersist := make(chan struct{})
	acknowledged := false
	consumer := &consumerStub{}
	consumer.receive = func(ctx context.Context) (Delivery, error) {
		if consumer.receiveCalls == 0 {
			consumer.receiveCalls++
			return Delivery{
				Event: testEvent(),
				Ack: func() error {
					acknowledged = true
					return nil
				},
			}, nil
		}
		<-ctx.Done()
		return Delivery{}, ctx.Err()
	}
	writer := &writerStub{insert: func(processingContext context.Context, _ event.Event) error {
		close(persistStarted)
		<-releasePersist
		if err := processingContext.Err(); err != nil {
			t.Errorf("processing context canceled before current delivery finished: %v", err)
		}
		return nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- New(consumer, writer, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))).Run(ctx)
	}()

	<-persistStarted
	cancel()
	if consumer.stopCount() != 0 {
		t.Error("consumer stopped before the current delivery finished")
	}
	close(releasePersist)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after the current delivery finished")
	}
	if consumer.stopCount() != 1 {
		t.Errorf("Stop() calls = %d, want 1", consumer.stopCount())
	}
	if !acknowledged {
		t.Error("completed delivery was not acknowledged")
	}
}

func TestWorkerRunReceiveError(t *testing.T) {
	receiveError := errors.New("consumer unavailable")
	consumer := &consumerStub{
		receive: func(context.Context) (Delivery, error) {
			return Delivery{}, receiveError
		},
	}

	err := New(consumer, &writerStub{}, discardLogger()).Run(context.Background())
	if !errors.Is(err, receiveError) {
		t.Fatalf("Run() error = %v, want wrapped receive error", err)
	}
	if consumer.stopCalls != 0 {
		t.Errorf("Stop() calls = %d, want 0 after receive failure", consumer.stopCalls)
	}
}

func TestWorkerRunAcknowledgementError(t *testing.T) {
	ackError := errors.New("ack failed")
	consumer := &consumerStub{
		receive: func(context.Context) (Delivery, error) {
			return Delivery{Event: testEvent(), Ack: func() error { return ackError }}, nil
		},
	}

	err := New(consumer, &writerStub{}, discardLogger()).Run(context.Background())
	if !errors.Is(err, ackError) {
		t.Fatalf("Run() error = %v, want wrapped acknowledgement error", err)
	}
}

func TestWorkerRunPersistenceErrorDoesNotAcknowledge(t *testing.T) {
	persistError := errors.New("ClickHouse unavailable")
	acknowledged := false
	consumer := &consumerStub{
		receive: func(context.Context) (Delivery, error) {
			return Delivery{
				Event: testEvent(),
				Ack: func() error {
					acknowledged = true
					return nil
				},
			}, nil
		},
	}
	writer := &writerStub{insert: func(context.Context, event.Event) error {
		return persistError
	}}

	err := New(consumer, writer, discardLogger()).Run(context.Background())
	if !errors.Is(err, persistError) {
		t.Fatalf("Run() error = %v, want wrapped persistence error", err)
	}
	if acknowledged {
		t.Error("delivery was acknowledged after persistence failure")
	}
}

func TestWorkerRunStopError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopError := errors.New("cancel consumer failed")
	consumer := &consumerStub{
		receive: func(ctx context.Context) (Delivery, error) {
			return Delivery{}, ctx.Err()
		},
		stop: func() error { return stopError },
	}

	err := New(consumer, &writerStub{}, discardLogger()).Run(ctx)
	if !errors.Is(err, stopError) {
		t.Fatalf("Run() error = %v, want wrapped stop error", err)
	}
}

func testEvent() event.Event {
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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type consumerStub struct {
	receive func(context.Context) (Delivery, error)
	stop    func() error

	mu           sync.Mutex
	receiveCalls int
	stopCalls    int
}

type writerStub struct {
	insert func(context.Context, event.Event) error
}

func (w *writerStub) Insert(ctx context.Context, applicationEvent event.Event) error {
	if w.insert == nil {
		return nil
	}
	return w.insert(ctx, applicationEvent)
}

func (c *consumerStub) Receive(ctx context.Context) (Delivery, error) {
	if c.receive == nil {
		return Delivery{}, errors.New("unexpected Receive call")
	}
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
