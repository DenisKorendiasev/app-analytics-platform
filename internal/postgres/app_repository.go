package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/app"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AppRepository persists App domain objects in PostgreSQL.
type AppRepository struct {
	pool *pgxpool.Pool
}

var _ app.Repository = (*AppRepository)(nil)

// NewAppRepository creates a PostgreSQL App repository.
func NewAppRepository(pool *pgxpool.Pool) *AppRepository {
	return &AppRepository{pool: pool}
}

// Create inserts an application.
func (r *AppRepository) Create(ctx context.Context, application app.App) error {
	const query = `
		INSERT INTO apps (id, name, publisher, category, created_at)
		VALUES ($1, $2, $3, $4, $5)`

	if _, err := r.pool.Exec(ctx, query,
		application.ID,
		application.Name,
		application.Publisher,
		application.Category,
		application.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert app %s: %w", application.ID, err)
	}
	return nil
}

// GetByID returns an application by ID.
func (r *AppRepository) GetByID(ctx context.Context, id uuid.UUID) (app.App, error) {
	const query = `
		SELECT id, name, publisher, category, created_at
		FROM apps
		WHERE id = $1`

	var application app.App
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&application.ID,
		&application.Name,
		&application.Publisher,
		&application.Category,
		&application.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.App{}, app.ErrNotFound
	}
	if err != nil {
		return app.App{}, fmt.Errorf("select app %s: %w", id, err)
	}
	application.CreatedAt = application.CreatedAt.UTC()
	return application, nil
}

// Exists reports whether an application exists.
func (r *AppRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM apps WHERE id = $1)`

	var exists bool
	if err := r.pool.QueryRow(ctx, query, id).Scan(&exists); err != nil {
		return false, fmt.Errorf("check app %s existence: %w", id, err)
	}
	return exists, nil
}
