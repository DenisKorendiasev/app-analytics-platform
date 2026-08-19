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

func TestRankingsRepository(t *testing.T) {
	if os.Getenv("CLICKHOUSE_INTEGRATION_TEST") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION_TEST=1 to run the Rankings repository integration test")
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

	databaseName := "rankings_repository_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	appA := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	appB := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	appC := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	appD := uuid.MustParse("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
	eventRepository := clickhouseinfra.NewEventRepository(connection)
	for _, applicationEvent := range rankingEvents(appA, appB, appC, appD) {
		if err := eventRepository.Insert(ctx, applicationEvent); err != nil {
			t.Fatalf("insert ranking fixture %s: %v", applicationEvent.EventID, err)
		}
	}

	fromAugust3 := time.Date(2026, time.August, 3, 0, 0, 0, 0, time.UTC)
	toAugust4 := time.Date(2026, time.August, 4, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		filter analytics.RankingFilter
		want   []analytics.Ranking
	}{
		{
			name:   "ranking order and stable tie break",
			filter: analytics.RankingFilter{Limit: 10},
			want: []analytics.Ranking{
				{AppID: appA, Value: 4},
				{AppID: appB, Value: 2},
				{AppID: appC, Value: 2},
				{AppID: appD, Value: 1},
			},
		},
		{
			name:   "country filter",
			filter: analytics.RankingFilter{Country: "US", Limit: 10},
			want:   []analytics.Ranking{{AppID: appA, Value: 1}},
		},
		{
			name: "date filter",
			filter: analytics.RankingFilter{
				From:        &fromAugust3,
				ToExclusive: &toAugust4,
				Limit:       10,
			},
			want: []analytics.Ranking{
				{AppID: appB, Value: 1},
				{AppID: appC, Value: 1},
				{AppID: appA, Value: 1},
			},
		},
		{
			name:   "limit",
			filter: analytics.RankingFilter{Limit: 2},
			want: []analytics.Ranking{
				{AppID: appA, Value: 4},
				{AppID: appB, Value: 2},
			},
		},
		{
			name:   "no matches",
			filter: analytics.RankingFilter{Country: "DE", Limit: 10},
			want:   []analytics.Ranking{},
		},
	}

	repository := clickhouseinfra.NewRankingsRepository(connection)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repository.ApplicationRankings(ctx, tt.filter)
			if err != nil {
				t.Fatalf("ApplicationRankings() error = %v", err)
			}
			assertRankings(t, got, tt.want)
		})
	}
}

func rankingEvents(appA, appB, appC, appD uuid.UUID) []event.Event {
	return []event.Event{
		rankingEvent(appA, event.TypeInstall, "RS", time.Date(2026, time.August, 1, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appA, event.TypeInstall, "RS", time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appA, event.TypeInstall, "RS", time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appA, event.TypeInstall, "US", time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appA, event.TypeSession, "RS", time.Date(2026, time.August, 3, 11, 0, 0, 0, time.UTC)),
		rankingEvent(appB, event.TypeInstall, "RS", time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appB, event.TypeInstall, "RS", time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appC, event.TypeInstall, "RS", time.Date(2026, time.August, 2, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appC, event.TypeInstall, "RS", time.Date(2026, time.August, 3, 10, 0, 0, 0, time.UTC)),
		rankingEvent(appD, event.TypeInstall, "RS", time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)),
	}
}

func rankingEvent(appID uuid.UUID, eventType event.Type, country string, timestamp time.Time) event.Event {
	return event.Event{
		EventID:   uuid.New(),
		AppID:     appID,
		EventType: eventType,
		Country:   country,
		Platform:  event.PlatformAndroid,
		Timestamp: timestamp,
	}
}

func assertRankings(t *testing.T, got, want []analytics.Ranking) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rankings count = %d, want %d; got = %+v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Errorf("ranking %d = %+v, want %+v", index, got[index], want[index])
		}
	}
}
