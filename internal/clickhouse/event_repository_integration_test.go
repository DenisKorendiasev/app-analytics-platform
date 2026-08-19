package clickhouse_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestEventRepository(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION_TEST") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION_TEST=1 to run the ClickHouse integration test")
	}

	applicationConfig, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	adminConfig := newClickHouseConfig(applicationConfig.ClickHouse)
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

	databaseName := "event_repository_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := adminConnection.Exec(ctx, "CREATE DATABASE "+databaseName); err != nil {
		t.Fatalf("create ClickHouse test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := adminConnection.Exec(cleanupContext, "DROP DATABASE IF EXISTS "+databaseName+" SYNC"); err != nil {
			t.Errorf("drop ClickHouse test database: %v", err)
		}
	})

	testConfig := newClickHouseConfig(applicationConfig.ClickHouse)
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

	applyMigration(t, ctx, connection, "../../migrations/clickhouse/000001_create_events.up.sql")
	assertTableDesign(t, ctx, connection)

	want := event.Event{
		EventID:      uuid.MustParse("785fe217-b25a-43bb-afac-aa94a4108959"),
		AppID:        uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"),
		EventType:    event.TypePurchase,
		Country:      "RS",
		Platform:     event.PlatformAndroid,
		RevenueCents: 999,
		Timestamp:    time.Date(2026, time.August, 18, 12, 35, 2, 123000000, time.UTC),
	}
	repository := clickhouseinfra.NewEventRepository(connection)
	if err := repository.Insert(ctx, want); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	const selectEvent = `
		SELECT event_id, app_id, event_type, country, platform, revenue_cents, timestamp
		FROM events
		WHERE event_id = ?`
	var (
		gotEventID      uuid.UUID
		gotAppID        uuid.UUID
		gotEventType    string
		gotCountry      string
		gotPlatform     string
		gotRevenueCents int64
		gotTimestamp    time.Time
	)
	if err := connection.QueryRow(ctx, selectEvent, want.EventID).Scan(
		&gotEventID,
		&gotAppID,
		&gotEventType,
		&gotCountry,
		&gotPlatform,
		&gotRevenueCents,
		&gotTimestamp,
	); err != nil {
		t.Fatalf("select inserted event: %v", err)
	}
	got := event.Event{
		EventID:      gotEventID,
		AppID:        gotAppID,
		EventType:    event.Type(gotEventType),
		Country:      gotCountry,
		Platform:     event.Platform(gotPlatform),
		RevenueCents: gotRevenueCents,
		Timestamp:    gotTimestamp.UTC(),
	}
	if got != want {
		t.Errorf("selected event = %+v, want %+v", got, want)
	}

	applyMigration(t, ctx, connection, "../../migrations/clickhouse/000001_create_events.down.sql")
	var tableCount uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'events'").Scan(&tableCount); err != nil {
		t.Fatalf("check down migration: %v", err)
	}
	if tableCount != 0 {
		t.Errorf("events table count = %d, want 0 after down migration", tableCount)
	}
}

func newClickHouseConfig(cfg config.ClickHouseConfig) clickhouseinfra.Config {
	return clickhouseinfra.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Database: cfg.Database,
		User:     cfg.User,
		Password: cfg.Password,
	}
}

func applyMigration(t *testing.T, ctx context.Context, connection driver.Conn, path string) {
	t.Helper()

	query, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration %s: %v", path, err)
	}
	if err := connection.Exec(ctx, string(query)); err != nil {
		t.Fatalf("apply migration %s: %v", path, err)
	}
}

func assertTableDesign(t *testing.T, ctx context.Context, connection driver.Conn) {
	t.Helper()

	var engine, sortingKey, partitionKey string
	const query = `
		SELECT engine, sorting_key, partition_key
		FROM system.tables
		WHERE database = currentDatabase() AND name = 'events'`
	if err := connection.QueryRow(ctx, query).Scan(&engine, &sortingKey, &partitionKey); err != nil {
		t.Fatalf("read events table design: %v", err)
	}
	if engine != "MergeTree" {
		t.Errorf("table engine = %q, want MergeTree", engine)
	}
	if sortingKey != "app_id, timestamp, event_id" {
		t.Errorf("sorting key = %q, want app_id, timestamp, event_id", sortingKey)
	}
	if partitionKey != "toYYYYMM(timestamp)" {
		t.Errorf("partition key = %q, want toYYYYMM(timestamp)", partitionKey)
	}
}
