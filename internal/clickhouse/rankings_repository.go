package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
)

// RankingsRepository reads ordered application install counts from ClickHouse.
type RankingsRepository struct {
	connection driver.Conn
}

var _ analytics.RankingsRepository = (*RankingsRepository)(nil)

// NewRankingsRepository creates a ClickHouse rankings repository.
func NewRankingsRepository(connection driver.Conn) *RankingsRepository {
	return &RankingsRepository{connection: connection}
}

// ApplicationRankings returns applications ordered by install count and app ID.
func (r *RankingsRepository) ApplicationRankings(ctx context.Context, filter analytics.RankingFilter) (result []analytics.Ranking, resultError error) {
	conditions := []string{"event_type = 'install'"}
	arguments := make([]any, 0, 4)
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
	arguments = append(arguments, filter.Limit)

	query := `
		SELECT app_id, count() AS value
		FROM events
		WHERE ` + strings.Join(conditions, " AND ") + `
		GROUP BY app_id
		ORDER BY value DESC, app_id ASC
		LIMIT ?`
	rows, err := r.connection.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query application rankings: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			resultError = errors.Join(resultError, fmt.Errorf("close application rankings rows: %w", err))
		}
		if err := rows.Err(); err != nil {
			resultError = errors.Join(resultError, fmt.Errorf("read application rankings: %w", err))
		}
	}()

	result = make([]analytics.Ranking, 0)
	for rows.Next() {
		var item analytics.Ranking
		if err := rows.Scan(&item.AppID, &item.Value); err != nil {
			return nil, fmt.Errorf("scan application ranking: %w", err)
		}
		result = append(result, item)
	}
	return result, nil
}
