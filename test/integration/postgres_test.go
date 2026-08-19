//go:build integration

package integration_test

import (
	"errors"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/app"
	postgresinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/google/uuid"
)

func TestPostgresAppRepository(t *testing.T) {
	ctx := integrationContext(t)
	pool := newPostgresPool(t, ctx)
	repository := postgresinfra.NewAppRepository(pool)

	application := app.App{
		ID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		Name:      "Spotify",
		Publisher: "Spotify AB",
		Category:  "music",
		CreatedAt: time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC),
	}

	t.Run("create read and existence", func(t *testing.T) {
		if err := repository.Create(ctx, application); err != nil {
			t.Fatalf("Create() error = %v", err)
		}

		got, err := repository.GetByID(ctx, application.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if got.ID != application.ID || got.Name != application.Name || got.Publisher != application.Publisher || got.Category != application.Category || !got.CreatedAt.Equal(application.CreatedAt) {
			t.Errorf("GetByID() = %+v, want %+v", got, application)
		}
		if got.CreatedAt.Location() != time.UTC {
			t.Errorf("GetByID() CreatedAt location = %s, want UTC", got.CreatedAt.Location())
		}

		exists, err := repository.Exists(ctx, application.ID)
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Error("Exists() = false, want true")
		}
	})

	t.Run("not found", func(t *testing.T) {
		unknownID := uuid.MustParse("f2ab71a7-a814-40d9-aa4c-c6f6541c1898")
		exists, err := repository.Exists(ctx, unknownID)
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Exists() = true, want false")
		}
		if _, err := repository.GetByID(ctx, unknownID); !errors.Is(err, app.ErrNotFound) {
			t.Errorf("GetByID() error = %v, want ErrNotFound", err)
		}
	})

	t.Run("constraints", func(t *testing.T) {
		tests := []struct {
			name        string
			application app.App
		}{
			{name: "empty name", application: constrainedApp("", "publisher", "category")},
			{name: "blank publisher", application: constrainedApp("name", "   ", "category")},
			{name: "empty category", application: constrainedApp("name", "publisher", "")},
			{
				name: "duplicate primary key",
				application: app.App{
					ID:        application.ID,
					Name:      "Duplicate",
					Publisher: "Publisher",
					Category:  "category",
					CreatedAt: time.Now().UTC(),
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if err := repository.Create(ctx, tt.application); err == nil {
					t.Error("Create() error = nil, want database constraint error")
				}
			})
		}
	})

	applyPostgresMigration(t, ctx, pool, "000001_create_apps.down.sql")
	var tableName *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass('apps')::text").Scan(&tableName); err != nil {
		t.Fatalf("check down migration: %v", err)
	}
	if tableName != nil {
		t.Errorf("apps table still exists after down migration: %s", *tableName)
	}
}

func constrainedApp(name, publisher, category string) app.App {
	return app.App{
		ID:        uuid.New(),
		Name:      name,
		Publisher: publisher,
		Category:  category,
		CreatedAt: time.Now().UTC(),
	}
}
