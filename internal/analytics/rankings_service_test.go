package analytics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRankingsServiceGetApplicationRankings(t *testing.T) {
	wantItems := []Ranking{
		{AppID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), Value: 12},
		{AppID: uuid.MustParse("22222222-2222-4222-8222-222222222222"), Value: 7},
	}
	repository := &rankingsRepositoryStub{rankings: func(_ context.Context, filter RankingFilter) ([]Ranking, error) {
		assertDate(t, filter.From, time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC), "From")
		assertDate(t, filter.ToExclusive, time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC), "ToExclusive")
		if filter.Country != "RS" {
			t.Errorf("filter Country = %q, want RS", filter.Country)
		}
		if filter.Limit != 2 {
			t.Errorf("filter Limit = %d, want 2", filter.Limit)
		}
		return wantItems, nil
	}}

	got, err := NewRankingsService(repository).GetApplicationRankings(context.Background(), RankingInput{
		Metric:  " installs ",
		Country: " RS ",
		From:    "2026-08-01",
		To:      "2026-08-18",
		Limit:   "2",
	})
	if err != nil {
		t.Fatalf("GetApplicationRankings() error = %v", err)
	}
	if got.Metric != RankingMetricInstalls {
		t.Errorf("Metric = %q, want installs", got.Metric)
	}
	if len(got.Items) != len(wantItems) {
		t.Fatalf("items count = %d, want %d", len(got.Items), len(wantItems))
	}
	for index := range wantItems {
		if got.Items[index] != wantItems[index] {
			t.Errorf("item %d = %+v, want %+v", index, got.Items[index], wantItems[index])
		}
	}
}

func TestRankingsServiceDefaults(t *testing.T) {
	repository := &rankingsRepositoryStub{rankings: func(_ context.Context, filter RankingFilter) ([]Ranking, error) {
		if filter.Limit != defaultRankingLimit {
			t.Errorf("filter Limit = %d, want %d", filter.Limit, defaultRankingLimit)
		}
		return nil, nil
	}}

	got, err := NewRankingsService(repository).GetApplicationRankings(context.Background(), RankingInput{})
	if err != nil {
		t.Fatalf("GetApplicationRankings() error = %v", err)
	}
	if got.Metric != RankingMetricInstalls {
		t.Errorf("Metric = %q, want installs", got.Metric)
	}
	if got.Items == nil || len(got.Items) != 0 {
		t.Errorf("Items = %#v, want empty non-nil slice", got.Items)
	}
}

func TestRankingsServiceValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   RankingInput
		wantErr error
	}{
		{name: "metric", input: RankingInput{Metric: "purchases"}, wantErr: ErrMetricInvalid},
		{name: "invalid from", input: RankingInput{From: "2026-02-30"}, wantErr: ErrFromInvalid},
		{name: "invalid to", input: RankingInput{To: "18-08-2026"}, wantErr: ErrToInvalid},
		{name: "reversed range", input: RankingInput{From: "2026-08-19", To: "2026-08-18"}, wantErr: ErrDateRangeInvalid},
		{name: "zero limit", input: RankingInput{Limit: "0"}, wantErr: ErrLimitInvalid},
		{name: "negative limit", input: RankingInput{Limit: "-1"}, wantErr: ErrLimitInvalid},
		{name: "large limit", input: RankingInput{Limit: "101"}, wantErr: ErrLimitInvalid},
		{name: "non-numeric limit", input: RankingInput{Limit: "ten"}, wantErr: ErrLimitInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &rankingsRepositoryStub{rankings: func(context.Context, RankingFilter) ([]Ranking, error) {
				t.Fatal("ApplicationRankings() called for invalid input")
				return nil, nil
			}}
			_, err := NewRankingsService(repository).GetApplicationRankings(context.Background(), tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetApplicationRankings() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRankingsServiceRepositoryError(t *testing.T) {
	repositoryError := errors.New("ClickHouse unavailable")
	repository := &rankingsRepositoryStub{rankings: func(context.Context, RankingFilter) ([]Ranking, error) {
		return nil, repositoryError
	}}

	_, err := NewRankingsService(repository).GetApplicationRankings(context.Background(), RankingInput{})
	if !errors.Is(err, repositoryError) {
		t.Fatalf("GetApplicationRankings() error = %v, want wrapped repository error", err)
	}
}

type rankingsRepositoryStub struct {
	rankings func(context.Context, RankingFilter) ([]Ranking, error)
}

func (r *rankingsRepositoryStub) ApplicationRankings(ctx context.Context, filter RankingFilter) ([]Ranking, error) {
	if r.rankings == nil {
		return nil, errors.New("unexpected ApplicationRankings call")
	}
	return r.rankings(ctx, filter)
}
