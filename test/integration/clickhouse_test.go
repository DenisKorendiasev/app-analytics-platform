//go:build integration

package integration_test

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestClickHouseEventRepository(t *testing.T) {
	ctx := integrationContext(t)
	connection := newClickHouseConnection(t, ctx)
	assertClickHouseTableDesign(t, connection)
	repository := clickhouseinfra.NewEventRepository(connection)

	first := integrationEvent(uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83"))
	second := integrationEvent(first.AppID)
	second.Timestamp = second.Timestamp.Add(time.Millisecond)
	third := integrationEvent(uuid.MustParse("d12f3583-65a7-477a-a664-3e04f93eaf43"))
	third.Timestamp = third.Timestamp.Add(2 * time.Millisecond)

	if err := repository.Insert(ctx, first); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := repository.InsertBatch(ctx, []event.Event{second, third}); err != nil {
		t.Fatalf("InsertBatch() error = %v", err)
	}

	var eventCount uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM events").Scan(&eventCount); err != nil {
		t.Fatalf("count inserted events: %v", err)
	}
	if eventCount != 3 {
		t.Errorf("inserted events = %d, want 3", eventCount)
	}
	assertClickHouseEvent(t, connection, first)

	applyClickHouseMigration(t, ctx, connection, "000001_create_events.down.sql")
	var tableCount uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'events'").Scan(&tableCount); err != nil {
		t.Fatalf("check ClickHouse down migration: %v", err)
	}
	if tableCount != 0 {
		t.Errorf("events table count = %d, want 0 after down migration", tableCount)
	}
}

func TestClickHouseStatisticsAggregation(t *testing.T) {
	ctx := integrationContext(t)
	connection := newClickHouseConnection(t, ctx)
	appID := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	otherAppID := uuid.MustParse("d12f3583-65a7-477a-a664-3e04f93eaf43")
	eventRepository := clickhouseinfra.NewEventRepository(connection)
	if err := eventRepository.InsertBatch(ctx, statisticsEvents(appID, otherAppID)); err != nil {
		t.Fatalf("insert statistics fixtures: %v", err)
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
		{name: "all events", appID: appID, want: analytics.Aggregates{Installs: 2, Sessions: 2, Purchases: 2, RevenueCents: 2499}},
		{
			name:  "date range",
			appID: appID,
			filter: analytics.Filter{
				From:        &fromAugust2,
				ToExclusive: &toAugust19,
			},
			want: analytics.Aggregates{Installs: 1, Sessions: 1, Purchases: 2, RevenueCents: 2499},
		},
		{name: "country", appID: appID, filter: analytics.Filter{Country: "RS"}, want: analytics.Aggregates{Installs: 2, Sessions: 2, Purchases: 1, RevenueCents: 999}},
		{name: "platform", appID: appID, filter: analytics.Filter{Platform: event.PlatformAndroid}, want: analytics.Aggregates{Installs: 1, Sessions: 2, Purchases: 1, RevenueCents: 1500}},
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
		{name: "no events", appID: uuid.New(), want: analytics.Aggregates{}},
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

func TestClickHouseRankingsAggregation(t *testing.T) {
	ctx := integrationContext(t)
	connection := newClickHouseConnection(t, ctx)
	appA := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	appB := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	appC := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	appD := uuid.MustParse("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
	eventRepository := clickhouseinfra.NewEventRepository(connection)
	if err := eventRepository.InsertBatch(ctx, rankingEvents(appA, appB, appC, appD)); err != nil {
		t.Fatalf("insert ranking fixtures: %v", err)
	}

	fromAugust3 := time.Date(2026, time.August, 3, 0, 0, 0, 0, time.UTC)
	toAugust4 := time.Date(2026, time.August, 4, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		filter analytics.RankingFilter
		want   []analytics.Ranking
	}{
		{
			name:   "order and stable tie break",
			filter: analytics.RankingFilter{Limit: 10},
			want: []analytics.Ranking{
				{AppID: appA, Value: 4},
				{AppID: appB, Value: 2},
				{AppID: appC, Value: 2},
				{AppID: appD, Value: 1},
			},
		},
		{name: "country", filter: analytics.RankingFilter{Country: "US", Limit: 10}, want: []analytics.Ranking{{AppID: appA, Value: 1}}},
		{
			name: "date range",
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
		{name: "limit", filter: analytics.RankingFilter{Limit: 2}, want: []analytics.Ranking{{AppID: appA, Value: 4}, {AppID: appB, Value: 2}}},
		{name: "no matches", filter: analytics.RankingFilter{Country: "DE", Limit: 10}, want: []analytics.Ranking{}},
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

func assertClickHouseTableDesign(t *testing.T, connection driver.Conn) {
	t.Helper()
	ctx := integrationContext(t)
	var engine, sortingKey, partitionKey string
	const query = `
		SELECT engine, sorting_key, partition_key
		FROM system.tables
		WHERE database = currentDatabase() AND name = 'events'`
	if err := connection.QueryRow(ctx, query).Scan(&engine, &sortingKey, &partitionKey); err != nil {
		t.Fatalf("read events table design: %v", err)
	}
	if engine != "MergeTree" || sortingKey != "app_id, timestamp, event_id" || partitionKey != "toYYYYMM(timestamp)" {
		t.Errorf("events table design = engine %q, sorting %q, partition %q", engine, sortingKey, partitionKey)
	}
}

func assertClickHouseEvent(t *testing.T, connection driver.Conn, want event.Event) {
	t.Helper()
	ctx := integrationContext(t)
	const query = `
		SELECT event_id, app_id, event_type, country, platform, revenue_cents, timestamp
		FROM events
		WHERE event_id = ?`
	var got event.Event
	var eventType, platform string
	if err := connection.QueryRow(ctx, query, want.EventID).Scan(
		&got.EventID,
		&got.AppID,
		&eventType,
		&got.Country,
		&platform,
		&got.RevenueCents,
		&got.Timestamp,
	); err != nil {
		t.Fatalf("select inserted event: %v", err)
	}
	got.EventType = event.Type(eventType)
	got.Platform = event.Platform(platform)
	got.Timestamp = got.Timestamp.UTC()
	if got != want {
		t.Errorf("inserted event = %+v, want %+v", got, want)
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
	return event.Event{EventID: uuid.New(), AppID: appID, EventType: eventType, Country: country, Platform: platform, RevenueCents: revenueCents, Timestamp: timestamp}
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
	return event.Event{EventID: uuid.New(), AppID: appID, EventType: eventType, Country: country, Platform: event.PlatformAndroid, Timestamp: timestamp}
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
