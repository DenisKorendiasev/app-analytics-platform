package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
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

func TestConsumerDeadLettersPoisonMessagesAndContinues(t *testing.T) {
	acknowledger := &acknowledgerStub{}
	deliveries := make(chan amqp.Delivery, 3)
	deliveries <- amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1, Body: []byte(`{"event_id":`)}
	invalidEvent := validConsumerEvent()
	invalidEvent.EventID = uuid.Nil
	invalidPayload, err := json.Marshal(invalidEvent)
	if err != nil {
		t.Fatalf("marshal invalid event: %v", err)
	}
	deliveries <- amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 2, Body: invalidPayload}
	want := validConsumerEvent()
	wantPayload, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal valid event: %v", err)
	}
	deliveries <- amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 3, Body: wantPayload}

	consumer := &Consumer{deliveries: deliveries}
	delivery, err := consumer.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if delivery.Event != want {
		t.Errorf("Receive() event = %+v, want %+v", delivery.Event, want)
	}
	if got := acknowledger.rejections(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("rejected delivery tags = %v, want [1 2]", got)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if got := acknowledger.acknowledgements(); len(got) != 1 || got[0] != 3 {
		t.Errorf("acknowledged delivery tags = %v, want [3]", got)
	}
}

func TestConsumerReturnsPoisonRejectionError(t *testing.T) {
	rejectionError := errors.New("channel closed")
	acknowledger := &acknowledgerStub{rejectError: rejectionError}
	deliveries := make(chan amqp.Delivery, 1)
	deliveries <- amqp.Delivery{Acknowledger: acknowledger, DeliveryTag: 1, Body: []byte(`{"event_id":`)}

	_, err := (&Consumer{deliveries: deliveries}).Receive(context.Background())
	if !errors.Is(err, rejectionError) || !strings.Contains(err.Error(), "decode RabbitMQ event") {
		t.Fatalf("Receive() error = %v, want decode and rejection errors", err)
	}
}

func validConsumerEvent() event.Event {
	return event.Event{
		EventID:      uuid.New(),
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 0, time.UTC),
	}
}

type acknowledgerStub struct {
	mu          sync.Mutex
	acked       []uint64
	rejected    []uint64
	rejectError error
}

func (a *acknowledgerStub) Ack(tag uint64, _ bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.acked = append(a.acked, tag)
	return nil
}

func (a *acknowledgerStub) Nack(uint64, bool, bool) error {
	return errors.New("unexpected Nack call")
}

func (a *acknowledgerStub) Reject(tag uint64, requeue bool) error {
	if requeue {
		return errors.New("unexpected requeue")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rejected = append(a.rejected, tag)
	return a.rejectError
}

func (a *acknowledgerStub) acknowledgements() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.acked...)
}

func (a *acknowledgerStub) rejections() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.rejected...)
}
