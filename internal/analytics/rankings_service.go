package analytics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

const (
	defaultRankingLimit = 10
	maximumRankingLimit = 100
)

// RankingsService validates ranking parameters and returns ordered applications.
type RankingsService struct {
	repository RankingsRepository
}

// NewRankingsService creates a RankingsService.
func NewRankingsService(repository RankingsRepository) *RankingsService {
	return &RankingsService{repository: repository}
}

// GetApplicationRankings returns applications ordered by the requested metric.
func (s *RankingsService) GetApplicationRankings(ctx context.Context, input RankingInput) (RankingResult, error) {
	metric := RankingMetric(strings.TrimSpace(input.Metric))
	if metric == "" {
		metric = RankingMetricInstalls
	}
	if metric != RankingMetricInstalls {
		return RankingResult{}, ErrMetricInvalid
	}

	filter, err := validateRankingFilter(input)
	if err != nil {
		return RankingResult{}, err
	}
	items, err := s.repository.ApplicationRankings(ctx, filter)
	if err != nil {
		return RankingResult{}, fmt.Errorf("get application rankings: %w", err)
	}
	if items == nil {
		items = make([]Ranking, 0)
	}
	return RankingResult{Metric: metric, Items: items}, nil
}

func validateRankingFilter(input RankingInput) (RankingFilter, error) {
	dateFilter, err := validateFilter(FilterInput{
		From:    input.From,
		To:      input.To,
		Country: input.Country,
	})
	if err != nil {
		return RankingFilter{}, err
	}

	limit := uint64(defaultRankingLimit)
	if value := strings.TrimSpace(input.Limit); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed < 1 || parsed > maximumRankingLimit {
			return RankingFilter{}, ErrLimitInvalid
		}
		limit = parsed
	}
	return RankingFilter{
		From:        dateFilter.From,
		ToExclusive: dateFilter.ToExclusive,
		Country:     dateFilter.Country,
		Limit:       limit,
	}, nil
}
