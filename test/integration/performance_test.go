//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/performance"
)

func TestPerformanceMeasurement(t *testing.T) {
	ctx := integrationContext(t)
	connection, err := clickhouseinfra.Open(ctx, suiteEnvironment.clickhouse)
	if err != nil {
		t.Fatalf("open ClickHouse source database: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close ClickHouse source database: %v", err)
		}
	})
	applyClickHouseMigration(t, ctx, connection, "000001_create_events.up.sql")
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := connection.Exec(cleanupContext, "DROP TABLE IF EXISTS events SYNC"); err != nil {
			t.Errorf("drop ClickHouse source events table: %v", err)
		}
	})

	before := temporaryPerformanceDatabaseCount(t, ctx, connection)
	report, err := performance.Measure(ctx, suiteEnvironment.clickhouse, performance.Options{
		EventCount: 10,
		Runs:       1,
		BatchSizes: []int{5, 10},
	})
	if err != nil {
		t.Fatalf("Measure() error = %v", err)
	}
	if report.EventCount != 10 || report.Runs != 1 || len(report.Measurements) != 2 {
		t.Errorf("measurement report header = %+v", report)
	}
	if report.AnalyticsQuery.DatasetEvents != 100000 {
		t.Errorf("analytics dataset events = %d, want 100000", report.AnalyticsQuery.DatasetEvents)
	}
	totalEvents := report.AnalyticsQuery.Result.Installs + report.AnalyticsQuery.Result.Sessions + report.AnalyticsQuery.Result.Purchases
	if totalEvents != 1000 {
		t.Errorf("analyzed application events = %d, want 1000", totalEvents)
	}
	if !planContains(report.AnalyticsQuery.Explain, "PrimaryKey") {
		t.Errorf("analytics explanation does not contain PrimaryKey: %v", report.AnalyticsQuery.Explain)
	}

	var sourceEvents uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM events").Scan(&sourceEvents); err != nil {
		t.Fatalf("count source events: %v", err)
	}
	if sourceEvents != 0 {
		t.Errorf("source event count = %d, want 0", sourceEvents)
	}
	if after := temporaryPerformanceDatabaseCount(t, ctx, connection); after != before {
		t.Errorf("temporary performance database count after run = %d, want baseline %d", after, before)
	}
}

func temporaryPerformanceDatabaseCount(t *testing.T, ctx context.Context, connection driver.Conn) uint64 {
	t.Helper()
	var count uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM system.databases WHERE startsWith(name, 'performance_')").Scan(&count); err != nil {
		t.Fatalf("count temporary performance databases: %v", err)
	}
	return count
}

func planContains(plan []string, text string) bool {
	for _, line := range plan {
		if strings.Contains(line, text) {
			return true
		}
	}
	return false
}
