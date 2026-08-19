package performance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
)

func TestRunnerRun(t *testing.T) {
	clock := &fakeClock{current: time.Unix(0, 0)}
	writer := &fakeWriter{clock: clock, insertDuration: 2 * time.Millisecond}
	resetCalls := 0
	runner := runner{
		writer: writer,
		reset: func(context.Context) error {
			resetCalls++
			return nil
		},
		events:     make([]event.Event, 5),
		batchSizes: []int{1, 2},
		runs:       2,
		now:        clock.now,
	}

	measurements, err := runner.run(context.Background())
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if resetCalls != 4 {
		t.Errorf("reset calls = %d, want 4", resetCalls)
	}
	if len(measurements) != 2 {
		t.Fatalf("measurement count = %d, want 2", len(measurements))
	}

	assertMeasurement(t, measurements[0], 1, 5, 10, 500)
	assertMeasurement(t, measurements[1], 2, 3, 6, 500.0/0.6)
	if len(writer.batchLengths) != 16 {
		t.Fatalf("insert call count = %d, want 16", len(writer.batchLengths))
	}
	wantLengths := []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 2, 2, 1}
	for index, want := range wantLengths {
		if writer.batchLengths[index] != want {
			t.Errorf("batch %d length = %d, want %d", index, writer.batchLengths[index], want)
		}
	}
}

func TestRunnerErrors(t *testing.T) {
	t.Run("reset", func(t *testing.T) {
		runner := runner{
			writer:     &fakeWriter{},
			reset:      func(context.Context) error { return errors.New("reset failed") },
			events:     make([]event.Event, 1),
			batchSizes: []int{1},
			runs:       1,
			now:        time.Now,
		}
		if _, err := runner.run(context.Background()); err == nil || !strings.Contains(err.Error(), "reset events before batch 1 run 1: reset failed") {
			t.Errorf("run() error = %v", err)
		}
	})

	t.Run("insert", func(t *testing.T) {
		insertError := errors.New("insert failed")
		runner := runner{
			writer:     &fakeWriter{err: insertError},
			reset:      func(context.Context) error { return nil },
			events:     make([]event.Event, 2),
			batchSizes: []int{1},
			runs:       1,
			now:        time.Now,
		}
		if _, err := runner.run(context.Background()); !errors.Is(err, insertError) {
			t.Errorf("run() error = %v, want insert error", err)
		}
	})
}

func TestValidateOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "events", options: Options{Runs: 1}, want: "event count"},
		{name: "runs", options: Options{EventCount: 1}, want: "run count"},
		{name: "batch size", options: Options{EventCount: 1, Runs: 1, BatchSizes: []int{0}}, want: "batch sizes"},
		{name: "duplicate", options: Options{EventCount: 1, Runs: 1, BatchSizes: []int{100, 100}}, want: "duplicated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateOptions(tt.options); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("validateOptions() error = %v, want text containing %q", err, tt.want)
			}
		})
	}

	options, err := validateOptions(Options{EventCount: 10, Runs: 2})
	if err != nil {
		t.Fatalf("validateOptions() error = %v", err)
	}
	if len(options.BatchSizes) != len(defaultBatchSizes) {
		t.Errorf("default batch sizes = %v", options.BatchSizes)
	}
}

func TestGenerateEvents(t *testing.T) {
	events, appID := generateEvents(301)
	if len(events) != 301 {
		t.Fatalf("event count = %d, want 301", len(events))
	}
	if events[0].AppID != appID || events[100].AppID != appID || events[300].AppID != appID {
		t.Error("analyzed application ID is not reused every 100 events")
	}
	for index, applicationEvent := range events {
		wantType := []event.Type{event.TypeInstall, event.TypeSession, event.TypePurchase}[index%3]
		if applicationEvent.EventType != wantType {
			t.Errorf("event %d type = %q, want %q", index, applicationEvent.EventType, wantType)
		}
		if wantType == event.TypePurchase && applicationEvent.RevenueCents <= 0 {
			t.Errorf("purchase event %d revenue = %d", index, applicationEvent.RevenueCents)
		}
		if wantType != event.TypePurchase && applicationEvent.RevenueCents != 0 {
			t.Errorf("non-purchase event %d revenue = %d", index, applicationEvent.RevenueCents)
		}
	}
}

func assertMeasurement(t *testing.T, measurement Measurement, batchSize, operations int, durationMS, eventsPerSecond float64) {
	t.Helper()
	if measurement.BatchSize != batchSize || measurement.InsertOperations != operations {
		t.Errorf("measurement identifiers = batch %d operations %d", measurement.BatchSize, measurement.InsertOperations)
	}
	if measurement.MedianProcessingDurationMS != float64(durationMS) {
		t.Errorf("processing duration = %fms, want %fms", measurement.MedianProcessingDurationMS, durationMS)
	}
	if measurement.MedianClickHouseInsertDurationMS != float64(durationMS) {
		t.Errorf("insert duration = %fms, want %fms", measurement.MedianClickHouseInsertDurationMS, durationMS)
	}
	if measurement.MedianEventsPerSecond != eventsPerSecond {
		t.Errorf("events/s = %f, want %f", measurement.MedianEventsPerSecond, eventsPerSecond)
	}
}

type fakeClock struct {
	current time.Time
}

func (c *fakeClock) now() time.Time {
	return c.current
}

type fakeWriter struct {
	clock          *fakeClock
	insertDuration time.Duration
	err            error
	batchLengths   []int
}

func (w *fakeWriter) InsertBatch(_ context.Context, events []event.Event) error {
	if w.err != nil {
		return w.err
	}
	w.batchLengths = append(w.batchLengths, len(events))
	if w.clock != nil {
		w.clock.current = w.clock.current.Add(w.insertDuration)
	}
	return nil
}
