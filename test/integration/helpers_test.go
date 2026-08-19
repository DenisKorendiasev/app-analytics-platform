//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	postgresinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationsDirectory = "../../migrations/"

func newPostgresPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	adminPool, err := postgresinfra.Open(ctx, suiteEnvironment.postgres)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := "integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	schemaIdentifier := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		t.Fatalf("create PostgreSQL test schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := adminPool.Exec(cleanupContext, "DROP SCHEMA "+schemaIdentifier+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL test schema: %v", err)
		}
	})

	poolConfig := adminPool.Config()
	runtimeParams := make(map[string]string, len(poolConfig.ConnConfig.RuntimeParams)+1)
	for key, value := range poolConfig.ConnConfig.RuntimeParams {
		runtimeParams[key] = value
	}
	runtimeParams["search_path"] = schemaName
	poolConfig.ConnConfig.RuntimeParams = runtimeParams

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create PostgreSQL schema pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping PostgreSQL schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyPostgresMigration(t, ctx, pool, "000001_create_apps.up.sql")
	return pool
}

func applyPostgresMigration(t *testing.T, ctx context.Context, pool *pgxpool.Pool, filename string) {
	t.Helper()
	query, err := os.ReadFile(migrationsDirectory + "postgres/" + filename)
	if err != nil {
		t.Fatalf("read PostgreSQL migration %s: %v", filename, err)
	}
	if _, err := pool.Exec(ctx, string(query)); err != nil {
		t.Fatalf("apply PostgreSQL migration %s: %v", filename, err)
	}
}

func newClickHouseConnection(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()

	adminConfig := suiteEnvironment.clickhouse
	adminConfig.Database = "default"
	adminConnection, err := clickhouseinfra.Open(ctx, adminConfig)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	t.Cleanup(func() {
		if err := adminConnection.Close(); err != nil {
			t.Errorf("close ClickHouse admin connection: %v", err)
		}
	})

	databaseName := "integration_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := adminConnection.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create ClickHouse test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := adminConnection.Exec(cleanupContext, "DROP DATABASE IF EXISTS "+databaseName+" SYNC"); err != nil {
			t.Errorf("drop ClickHouse test database: %v", err)
		}
	})

	testConfig := suiteEnvironment.clickhouse
	testConfig.Database = databaseName
	connection, err := clickhouseinfra.Open(ctx, testConfig)
	if err != nil {
		t.Fatalf("open ClickHouse test connection: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close ClickHouse test connection: %v", err)
		}
	})

	applyClickHouseMigration(t, ctx, connection, "000001_create_events.up.sql")
	return connection
}

func applyClickHouseMigration(t *testing.T, ctx context.Context, connection driver.Conn, filename string) {
	t.Helper()
	query, err := os.ReadFile(migrationsDirectory + "clickhouse/" + filename)
	if err != nil {
		t.Fatalf("read ClickHouse migration %s: %v", filename, err)
	}
	if err := connection.Exec(ctx, string(query)); err != nil {
		t.Fatalf("apply ClickHouse migration %s: %v", filename, err)
	}
}

func newRabbitConfig() rabbitmq.Config {
	testID := uuid.NewString()
	return rabbitmq.Config{
		URL:        suiteEnvironment.rabbitURL,
		Exchange:   fmt.Sprintf("app.events.integration.%s", testID),
		Queue:      fmt.Sprintf("app.events.integration.%s", testID),
		RoutingKey: "app.events.integration",
	}
}
