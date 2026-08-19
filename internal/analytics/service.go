package analytics

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

const dateLayout = "2006-01-02"

// Service validates filters and returns application statistics.
type Service struct {
	repository Repository
}

// NewService creates an analytics Service.
func NewService(repository Repository) *Service {
	return &Service{repository: repository}
}

// GetApplicationStatistics returns filtered event metrics for one application.
func (s *Service) GetApplicationStatistics(ctx context.Context, appID uuid.UUID, input FilterInput) (Statistics, error) {
	if appID == uuid.Nil {
		return Statistics{}, ErrAppIDRequired
	}

	filter, err := validateFilter(input)
	if err != nil {
		return Statistics{}, err
	}
	aggregates, err := s.repository.ApplicationStatistics(ctx, appID, filter)
	if err != nil {
		return Statistics{}, fmt.Errorf("get application %s statistics: %w", appID, err)
	}
	return Statistics{
		AppID:        appID,
		Installs:     aggregates.Installs,
		Sessions:     aggregates.Sessions,
		Purchases:    aggregates.Purchases,
		RevenueCents: aggregates.RevenueCents,
	}, nil
}

func validateFilter(input FilterInput) (Filter, error) {
	from, err := parseDate(input.From, ErrFromInvalid)
	if err != nil {
		return Filter{}, err
	}
	to, err := parseDate(input.To, ErrToInvalid)
	if err != nil {
		return Filter{}, err
	}
	if from != nil && to != nil && from.After(*to) {
		return Filter{}, ErrDateRangeInvalid
	}

	platform := event.Platform(strings.TrimSpace(input.Platform))
	if platform != "" && platform != event.PlatformAndroid && platform != event.PlatformIOS {
		return Filter{}, ErrPlatformInvalid
	}

	filter := Filter{
		From:     from,
		Country:  strings.TrimSpace(input.Country),
		Platform: platform,
	}
	if to != nil {
		toExclusive := to.AddDate(0, 0, 1)
		filter.ToExclusive = &toExclusive
	}
	return filter, nil
}

func parseDate(value string, validationError error) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(dateLayout, value)
	if err != nil {
		return nil, validationError
	}
	return &parsed, nil
}
