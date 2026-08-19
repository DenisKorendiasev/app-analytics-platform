package clickhouse_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestStatisticsRepository(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION_TEST") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION_TEST=1 to run the Statistics repository integration test")
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

	databaseName := "statistics_repository_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	appID := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	otherAppID := uuid.MustParse("d12f3583-65a7-477a-a664-3e04f93eaf43")
	eventRepository := clickhouseinfra.NewEventRepository(connection)
	for _, applicationEvent := range statisticsEvents(appID, otherAppID) {
		if err := eventRepository.Insert(ctx, applicationEvent); err != nil {
			t.Fatalf("insert statistics fixture %s: %v", applicationEvent.EventID, err)
		}
	}

	fromAugust2 := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC)
	fromAugust3 := time.Date(2026, time.August, 3, 0, 0, 0, 0, time.UTC)
	toAugust19 := time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		appID  uuid.UUID
		filter analytics.Filter
		want   analytics.Aggregates
	}{
		{
			name:  "all application events",
			appID: appID,
			want:  analytics.Aggregates{Installs: 2, Sessions: 2, Purchases: 2, RevenueCents: 2499},
		},
		{
			name:  "inclusive date range",
			appID: appID,
			filter: analytics.Filter{
				From:        &fromAugust2,
				ToExclusive: &toAugust19,
			},
			want: analytics.Aggregates{Installs: 1, Sessions: 1, Purchases: 2, RevenueCents: 2499},
		},
		{
			name:   "country",
			appID:  appID,
			filter: analytics.Filter{Country: "RS"},
			want:   analytics.Aggregates{Installs: 2, Sessions: 2, Purchases: 1, RevenueCents: 999},
		},
		{
			name:   "platform",
			appID:  appID,
			filter: analytics.Filter{Platform: event.PlatformAndroid},
			want:   analytics.Aggregates{Installs: 1, Sessions: 2, Purchases: 1, RevenueCents: 1500},
		},
		{
			name:  "combined filters",
			appID: appID,
			filter: analytics.Filter{
				From:        &fromAugust3,
				ToExclusive: &toAugust19,
				Country:     "RS",
				Platform:    event.PlatformIOS,
			},
			want: analytics.Aggregates{Installs: 1, Purchases: 1, RevenueCents: 999},
		},
		{
			name:  "application without events",
			appID: uuid.New(),
			want:  analytics.Aggregates{},
		},
	}

	repository := clickhouseinfra.NewStatisticsRepository(connection)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repository.ApplicationStatistics(ctx, tt.appID, tt.filter)
			if err != nil {
				t.Fatalf("ApplicationStatistics() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("ApplicationStatistics() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func statisticsEvents(appID, otherAppID uuid.UUID) []event.Event {
	return []event.Event{
		statisticsEvent(appID, event.TypeInstall, "RS", event.PlatformAndroid, 0, time.Date(2026, time.August, 1, 10, 0, 0, 0, time.UTC)),
		statisticsEvent(appID, event.TypeSession, "RS", event.PlatformAndroid, 0, time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)),
		statisticsEvent(appID, event.TypePurchase, "RS", event.PlatformIOS, 999, time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)),
		statisticsEvent(appID, event.TypePurchase, "US", event.PlatformAndroid, 1500, time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC)),
		statisticsEvent(appID, event.TypeInstall, "RS", event.PlatformIOS, 0, time.Date(2026, time.August, 18, 23, 59, 59, 999000000, time.UTC)),
		statisticsEvent(appID, event.TypeSession, "RS", event.PlatformAndroid, 0, time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC)),
		statisticsEvent(otherAppID, event.TypePurchase, "RS", event.PlatformAndroid, 700, time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)),
	}
}

func statisticsEvent(appID uuid.UUID, eventType event.Type, country string, platform event.Platform, revenueCents int64, timestamp time.Time) event.Event {
	return event.Event{
		EventID:      uuid.New(),
		AppID:        appID,
		EventType:    eventType,
		Country:      country,
		Platform:     platform,
		RevenueCents: revenueCents,
		Timestamp:    timestamp,
	}
}
