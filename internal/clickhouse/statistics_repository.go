package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
	"github.com/google/uuid"
)

// StatisticsRepository reads application event aggregates from ClickHouse.
type StatisticsRepository struct {
	connection driver.Conn
}

var _ analytics.Repository = (*StatisticsRepository)(nil)

// NewStatisticsRepository creates a ClickHouse statistics repository.
func NewStatisticsRepository(connection driver.Conn) *StatisticsRepository {
	return &StatisticsRepository{connection: connection}
}

// ApplicationStatistics aggregates event counts and purchase revenue.
func (r *StatisticsRepository) ApplicationStatistics(ctx context.Context, appID uuid.UUID, filter analytics.Filter) (analytics.Aggregates, error) {
	conditions := []string{"app_id = ?"}
	arguments := []any{appID}
	if filter.From != nil {
		conditions = append(conditions, "timestamp >= ?")
		arguments = append(arguments, *filter.From)
	}
	if filter.ToExclusive != nil {
		conditions = append(conditions, "timestamp < ?")
		arguments = append(arguments, *filter.ToExclusive)
	}
	if filter.Country != "" {
		conditions = append(conditions, "country = ?")
		arguments = append(arguments, filter.Country)
	}
	if filter.Platform != "" {
		conditions = append(conditions, "platform = ?")
		arguments = append(arguments, string(filter.Platform))
	}

	query := `
		SELECT
			countIf(event_type = 'install'),
			countIf(event_type = 'session'),
			countIf(event_type = 'purchase'),
			sumIf(revenue_cents, event_type = 'purchase')
		FROM events
		WHERE ` + strings.Join(conditions, " AND ")

	var result analytics.Aggregates
	if err := r.connection.QueryRow(ctx, query, arguments...).Scan(
		&result.Installs,
		&result.Sessions,
		&result.Purchases,
		&result.RevenueCents,
	); err != nil {
		return analytics.Aggregates{}, fmt.Errorf("query application %s statistics: %w", appID, err)
	}
	return result, nil
}
