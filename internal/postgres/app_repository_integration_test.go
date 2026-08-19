package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/app"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationsDirectory = "../../migrations/postgres/"

func TestAppRepository(t *testing.T) {
	adminPool := openIntegrationPool(t)
	t.Cleanup(adminPool.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	schemaName := fmt.Sprintf("app_repository_test_%x", uuid.New())
	schemaIdentifier := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := adminPool.Exec(cleanupContext, "DROP SCHEMA "+schemaIdentifier+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})

	poolConfig := adminPool.Config()
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schemaName
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create test schema pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigration(t, ctx, pool, migrationsDirectory+"000001_create_apps.up.sql")
	repository := postgres.NewAppRepository(pool)

	application := app.App{
		ID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		Name:      "Spotify",
		Publisher: "Spotify AB",
		Category:  "music",
		CreatedAt: time.Date(2026, time.August, 18, 10, 0, 0, 0, time.UTC),
	}
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

	unknownID := uuid.MustParse("f2ab71a7-a814-40d9-aa4c-c6f6541c1898")
	exists, err = repository.Exists(ctx, unknownID)
	if err != nil {
		t.Fatalf("Exists() for unknown app error = %v", err)
	}
	if exists {
		t.Error("Exists() for unknown app = true, want false")
	}
	if _, err := repository.GetByID(ctx, unknownID); !errors.Is(err, app.ErrNotFound) {
		t.Errorf("GetByID() for unknown app error = %v, want ErrNotFound", err)
	}

	applyMigration(t, ctx, pool, migrationsDirectory+"000001_create_apps.down.sql")
	var tableName *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass('apps')::text").Scan(&tableName); err != nil {
		t.Fatalf("check down migration: %v", err)
	}
	if tableName != nil {
		t.Errorf("apps table still exists after down migration: %s", *tableName)
	}
}

func applyMigration(t *testing.T, ctx context.Context, pool *pgxpool.Pool, path string) {
	t.Helper()

	query, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	if _, err := pool.Exec(ctx, string(query)); err != nil {
		t.Fatalf("apply migration %s: %v", path, err)
	}
}
