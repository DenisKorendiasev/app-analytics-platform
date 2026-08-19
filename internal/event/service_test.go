package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestServiceIngest(t *testing.T) {
	fixedID := uuid.MustParse("785fe217-b25a-43bb-afac-aa94a4108959")
	input := validInput()
	input.Country = " RS "
	input.Timestamp = time.Date(2026, time.August, 18, 15, 0, 0, 0, time.FixedZone("test", 3*60*60))

	var checkedAppID uuid.UUID
	repository := &appRepositoryStub{
		exists: func(_ context.Context, appID uuid.UUID) (bool, error) {
			checkedAppID = appID
			return true, nil
		},
	}
	var published Event
	publisher := &publisherStub{
		publish: func(_ context.Context, applicationEvent Event) error {
			published = applicationEvent
			return nil
		},
	}
	service := NewService(repository, publisher)
	service.newID = func() uuid.UUID { return fixedID }

	got, err := service.Ingest(context.Background(), input)
	if err != nil {
		t.Fatalf("Ingest() error = %v", err)
	}

	if checkedAppID != input.AppID {
		t.Errorf("Exists() app ID = %s, want %s", checkedAppID, input.AppID)
	}
	if got.EventID != fixedID {
		t.Errorf("EventID = %s, want %s", got.EventID, fixedID)
	}
	if got.AppID != input.AppID || got.EventType != input.EventType || got.Platform != input.Platform || got.RevenueCents != input.RevenueCents {
		t.Errorf("event = %+v, want input fields from %+v", got, input)
	}
	if got.Country != "RS" {
		t.Errorf("Country = %q, want RS", got.Country)
	}
	if !got.Timestamp.Equal(input.Timestamp) || got.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp = %v, want %v in UTC", got.Timestamp, input.Timestamp.UTC())
	}
	if published != got {
		t.Errorf("published event = %+v, want %+v", published, got)
	}
}

func TestServiceIngestValidation(t *testing.T) {
	tests := []struct {
		name      string
		change    func(*Input)
		wantError error
	}{
		{name: "missing app ID", change: func(input *Input) { input.AppID = uuid.Nil }, wantError: ErrAppIDRequired},
		{name: "invalid type", change: func(input *Input) { input.EventType = "login" }, wantError: ErrEventTypeInvalid},
		{name: "missing country", change: func(input *Input) { input.Country = "  " }, wantError: ErrCountryRequired},
		{name: "invalid platform", change: func(input *Input) { input.Platform = "windows" }, wantError: ErrPlatformInvalid},
		{name: "negative purchase revenue", change: func(input *Input) { input.RevenueCents = -1 }, wantError: ErrPurchaseRevenueInvalid},
		{name: "missing timestamp", change: func(input *Input) { input.Timestamp = time.Time{} }, wantError: ErrTimestampRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			tt.change(&input)
			repository := &appRepositoryStub{
				exists: func(_ context.Context, _ uuid.UUID) (bool, error) {
					t.Fatal("Exists() called for invalid input")
					return false, nil
				},
			}
			publisher := &publisherStub{
				publish: func(_ context.Context, _ Event) error {
					t.Fatal("Publish() called for invalid input")
					return nil
				},
			}

			_, err := NewService(repository, publisher).Ingest(context.Background(), input)
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("Ingest() error = %v, want %v", err, tt.wantError)
			}
		})
	}
}

func TestServiceIngestAllowsNegativeRevenueForNonPurchase(t *testing.T) {
	input := validInput()
	input.EventType = TypeSession
	input.RevenueCents = -1
	service := NewService(
		&appRepositoryStub{exists: func(_ context.Context, _ uuid.UUID) (bool, error) { return true, nil }},
		&publisherStub{publish: func(_ context.Context, _ Event) error { return nil }},
	)

	if _, err := service.Ingest(context.Background(), input); err != nil {
		t.Fatalf("Ingest() error = %v, want non-purchase revenue to be outside purchase validation", err)
	}
}

func TestServiceIngestMissingApp(t *testing.T) {
	publisher := &publisherStub{
		publish: func(_ context.Context, _ Event) error {
			t.Fatal("Publish() called for an unknown app")
			return nil
		},
	}
	service := NewService(
		&appRepositoryStub{exists: func(_ context.Context, _ uuid.UUID) (bool, error) { return false, nil }},
		publisher,
	)

	_, err := service.Ingest(context.Background(), validInput())
	if !errors.Is(err, ErrAppNotFound) {
		t.Fatalf("Ingest() error = %v, want ErrAppNotFound", err)
	}
}

func TestServiceIngestRepositoryError(t *testing.T) {
	repositoryError := errors.New("database unavailable")
	service := NewService(
		&appRepositoryStub{exists: func(_ context.Context, _ uuid.UUID) (bool, error) { return false, repositoryError }},
		&publisherStub{},
	)

	_, err := service.Ingest(context.Background(), validInput())
	if !errors.Is(err, repositoryError) {
		t.Fatalf("Ingest() error = %v, want wrapped repository error", err)
	}
}

func TestServiceIngestPublisherError(t *testing.T) {
	publisherError := errors.New("broker unavailable")
	service := NewService(
		&appRepositoryStub{exists: func(_ context.Context, _ uuid.UUID) (bool, error) { return true, nil }},
		&publisherStub{publish: func(_ context.Context, _ Event) error { return publisherError }},
	)

	_, err := service.Ingest(context.Background(), validInput())
	if !errors.Is(err, publisherError) {
		t.Fatalf("Ingest() error = %v, want wrapped publisher error", err)
	}
}

func validInput() Input {
	return Input{
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    TypePurchase,
		Country:      "RS",
		Platform:     PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 0, time.UTC),
	}
}

type appRepositoryStub struct {
	exists func(context.Context, uuid.UUID) (bool, error)
}

func (r *appRepositoryStub) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	if r.exists == nil {
		return false, errors.New("unexpected Exists call")
	}
	return r.exists(ctx, id)
}

type publisherStub struct {
	publish func(context.Context, Event) error
}

func (p *publisherStub) Publish(ctx context.Context, applicationEvent Event) error {
	if p.publish == nil {
		return errors.New("unexpected Publish call")
	}
	return p.publish(ctx, applicationEvent)
}
