package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
)

// EventRepository persists Event domain objects in ClickHouse.
type EventRepository struct {
	connection driver.Conn
}

// NewEventRepository creates a ClickHouse Event repository.
func NewEventRepository(connection driver.Conn) *EventRepository {
	return &EventRepository{connection: connection}
}

// Insert stores one event in the analytical events table.
func (r *EventRepository) Insert(ctx context.Context, applicationEvent event.Event) error {
	return r.InsertBatch(ctx, []event.Event{applicationEvent})
}

// InsertBatch stores events using one native ClickHouse batch.
func (r *EventRepository) InsertBatch(ctx context.Context, events []event.Event) (result error) {
	if len(events) == 0 {
		return nil
	}

	const query = `
		INSERT INTO events (
			event_id,
			app_id,
			event_type,
			country,
			platform,
			revenue_cents,
			timestamp
		)`

	batch, err := r.connection.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare event batch of %d: %w", len(events), err)
	}
	defer func() {
		if err := batch.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close event batch of %d: %w", len(events), err))
		}
	}()

	for _, applicationEvent := range events {
		if err := batch.Append(
			applicationEvent.EventID,
			applicationEvent.AppID,
			string(applicationEvent.EventType),
			applicationEvent.Country,
			string(applicationEvent.Platform),
			applicationEvent.RevenueCents,
			applicationEvent.Timestamp.UTC(),
		); err != nil {
			return fmt.Errorf("append event %s to batch: %w", applicationEvent.EventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send event batch of %d: %w", len(events), err)
	}
	return nil
}
