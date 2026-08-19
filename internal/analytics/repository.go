package analytics

import (
	"context"

	"github.com/google/uuid"
)

// Repository provides application event aggregates.
type Repository interface {
	ApplicationStatistics(ctx context.Context, appID uuid.UUID, filter Filter) (Aggregates, error)
}
