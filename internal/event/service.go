package event

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// AppRepository provides the application lookup required by event ingestion.
type AppRepository interface {
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
}

// Service validates and publishes application events.
type Service struct {
	apps      AppRepository
	publisher Publisher
	newID     func() uuid.UUID
}

// NewService creates an Event service.
func NewService(apps AppRepository, publisher Publisher) *Service {
	return &Service{
		apps:      apps,
		publisher: publisher,
		newID:     uuid.New,
	}
}

// Ingest validates an event, verifies its application, and publishes it.
func (s *Service) Ingest(ctx context.Context, input Input) (Event, error) {
	if input.AppID == uuid.Nil {
		return Event{}, ErrAppIDRequired
	}
	if input.EventType != TypeInstall && input.EventType != TypeSession && input.EventType != TypePurchase {
		return Event{}, ErrEventTypeInvalid
	}

	input.Country = strings.TrimSpace(input.Country)
	if input.Country == "" {
		return Event{}, ErrCountryRequired
	}
	if input.Platform != PlatformAndroid && input.Platform != PlatformIOS {
		return Event{}, ErrPlatformInvalid
	}
	if input.EventType == TypePurchase && input.RevenueCents < 0 {
		return Event{}, ErrPurchaseRevenueInvalid
	}
	if input.Timestamp.IsZero() {
		return Event{}, ErrTimestampRequired
	}

	exists, err := s.apps.Exists(ctx, input.AppID)
	if err != nil {
		return Event{}, fmt.Errorf("check event app existence: %w", err)
	}
	if !exists {
		return Event{}, ErrAppNotFound
	}

	acceptedEvent := Event{
		EventID:      s.newID(),
		AppID:        input.AppID,
		EventType:    input.EventType,
		Country:      input.Country,
		Platform:     input.Platform,
		RevenueCents: input.RevenueCents,
		Timestamp:    input.Timestamp.UTC(),
	}
	if err := s.publisher.Publish(ctx, acceptedEvent); err != nil {
		return Event{}, fmt.Errorf("publish event: %w", err)
	}

	return acceptedEvent, nil
}
