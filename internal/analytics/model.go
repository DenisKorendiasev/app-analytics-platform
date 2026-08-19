// Package analytics contains application statistics business logic and HTTP handling.
package analytics

import (
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

// Statistics contains aggregated event metrics for one application.
type Statistics struct {
	AppID        uuid.UUID
	Installs     uint64
	Sessions     uint64
	Purchases    uint64
	RevenueCents int64
}

// Aggregates contains metrics returned by the analytical repository.
type Aggregates struct {
	Installs     uint64
	Sessions     uint64
	Purchases    uint64
	RevenueCents int64
}

// FilterInput contains raw optional values accepted by the Statistics API.
type FilterInput struct {
	From     string
	To       string
	Country  string
	Platform string
}

// Filter contains validated repository filters. ToExclusive is the beginning
// of the day after the inclusive API `to` date.
type Filter struct {
	From        *time.Time
	ToExclusive *time.Time
	Country     string
	Platform    event.Platform
}

// RankingMetric identifies the aggregate used to order applications.
type RankingMetric string

const (
	// RankingMetricInstalls orders applications by install count.
	RankingMetricInstalls RankingMetric = "installs"
)

// Ranking contains one application and its metric value.
type Ranking struct {
	AppID uuid.UUID
	Value uint64
}

// RankingResult contains an ordered application ranking.
type RankingResult struct {
	Metric RankingMetric
	Items  []Ranking
}

// RankingInput contains raw optional Rankings API values.
type RankingInput struct {
	Metric  string
	Country string
	From    string
	To      string
	Limit   string
}

// RankingFilter contains validated ClickHouse ranking filters.
type RankingFilter struct {
	From        *time.Time
	ToExclusive *time.Time
	Country     string
	Limit       uint64
}
