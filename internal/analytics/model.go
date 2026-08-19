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
