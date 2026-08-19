package app

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines persistence operations required by the App domain.
type Repository interface {
	Create(ctx context.Context, application App) error
	GetByID(ctx context.Context, id uuid.UUID) (App, error)
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
}
