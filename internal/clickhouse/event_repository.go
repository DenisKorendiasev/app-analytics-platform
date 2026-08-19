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
func (r *EventRepository) Insert(ctx context.Context, applicationEvent event.Event) (result error) {
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
		return fmt.Errorf("prepare event %s insert: %w", applicationEvent.EventID, err)
	}
	defer func() {
		if err := batch.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close event %s insert batch: %w", applicationEvent.EventID, err))
		}
	}()

	if err := batch.Append(
		applicationEvent.EventID,
		applicationEvent.AppID,
		string(applicationEvent.EventType),
		applicationEvent.Country,
		string(applicationEvent.Platform),
		applicationEvent.RevenueCents,
		applicationEvent.Timestamp.UTC(),
	); err != nil {
		return fmt.Errorf("append event %s insert: %w", applicationEvent.EventID, err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send event %s insert: %w", applicationEvent.EventID, err)
	}
	return nil
}
