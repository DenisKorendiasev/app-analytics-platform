package analytics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestServiceGetApplicationStatistics(t *testing.T) {
	appID := uuid.MustParse("b8edbe8d-4fa6-42fd-a351-9a98d17d8b83")
	wantAggregates := Aggregates{Installs: 12, Sessions: 34, Purchases: 5, RevenueCents: 6789}
	repository := &repositoryStub{statistics: func(_ context.Context, gotAppID uuid.UUID, filter Filter) (Aggregates, error) {
		if gotAppID != appID {
			t.Errorf("ApplicationStatistics() app ID = %s, want %s", gotAppID, appID)
		}
		assertDate(t, filter.From, time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC), "From")
		assertDate(t, filter.ToExclusive, time.Date(2026, time.August, 19, 0, 0, 0, 0, time.UTC), "ToExclusive")
		if filter.Country != "RS" {
			t.Errorf("filter Country = %q, want RS", filter.Country)
		}
		if filter.Platform != event.PlatformAndroid {
			t.Errorf("filter Platform = %q, want android", filter.Platform)
		}
		return wantAggregates, nil
	}}

	got, err := NewService(repository).GetApplicationStatistics(context.Background(), appID, FilterInput{
		From:     " 2026-08-01 ",
		To:       "2026-08-18",
		Country:  " RS ",
		Platform: "android",
	})
	if err != nil {
		t.Fatalf("GetApplicationStatistics() error = %v", err)
	}
	want := Statistics{
		AppID:        appID,
		Installs:     wantAggregates.Installs,
		Sessions:     wantAggregates.Sessions,
		Purchases:    wantAggregates.Purchases,
		RevenueCents: wantAggregates.RevenueCents,
	}
	if got != want {
		t.Errorf("GetApplicationStatistics() = %+v, want %+v", got, want)
	}
}

func TestServiceGetApplicationStatisticsWithoutFilters(t *testing.T) {
	appID := uuid.New()
	repository := &repositoryStub{statistics: func(_ context.Context, _ uuid.UUID, filter Filter) (Aggregates, error) {
		if filter.From != nil || filter.ToExclusive != nil || filter.Country != "" || filter.Platform != "" {
			t.Errorf("filter = %+v, want empty", filter)
		}
		return Aggregates{}, nil
	}}

	if _, err := NewService(repository).GetApplicationStatistics(context.Background(), appID, FilterInput{}); err != nil {
		t.Fatalf("GetApplicationStatistics() error = %v", err)
	}
}

func TestServiceGetApplicationStatisticsValidation(t *testing.T) {
	tests := []struct {
		name    string
		appID   uuid.UUID
		input   FilterInput
		wantErr error
	}{
		{name: "empty app ID", appID: uuid.Nil, wantErr: ErrAppIDRequired},
		{name: "invalid from", appID: uuid.New(), input: FilterInput{From: "2026-02-30"}, wantErr: ErrFromInvalid},
		{name: "invalid to", appID: uuid.New(), input: FilterInput{To: "18-08-2026"}, wantErr: ErrToInvalid},
		{name: "reversed range", appID: uuid.New(), input: FilterInput{From: "2026-08-19", To: "2026-08-18"}, wantErr: ErrDateRangeInvalid},
		{name: "invalid platform", appID: uuid.New(), input: FilterInput{Platform: "windows"}, wantErr: ErrPlatformInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repository := &repositoryStub{statistics: func(context.Context, uuid.UUID, Filter) (Aggregates, error) {
				t.Fatal("ApplicationStatistics() called for invalid input")
				return Aggregates{}, nil
			}}
			_, err := NewService(repository).GetApplicationStatistics(context.Background(), tt.appID, tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("GetApplicationStatistics() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestServiceGetApplicationStatisticsRepositoryError(t *testing.T) {
	repositoryError := errors.New("ClickHouse unavailable")
	repository := &repositoryStub{statistics: func(context.Context, uuid.UUID, Filter) (Aggregates, error) {
		return Aggregates{}, repositoryError
	}}

	_, err := NewService(repository).GetApplicationStatistics(context.Background(), uuid.New(), FilterInput{})
	if !errors.Is(err, repositoryError) {
		t.Fatalf("GetApplicationStatistics() error = %v, want wrapped repository error", err)
	}
}

func assertDate(t *testing.T, got *time.Time, want time.Time, field string) {
	t.Helper()
	if got == nil || !got.Equal(want) {
		t.Errorf("filter %s = %v, want %s", field, got, want)
	}
}

type repositoryStub struct {
	statistics func(context.Context, uuid.UUID, Filter) (Aggregates, error)
}

func (r *repositoryStub) ApplicationStatistics(ctx context.Context, appID uuid.UUID, filter Filter) (Aggregates, error) {
	if r.statistics == nil {
		return Aggregates{}, errors.New("unexpected ApplicationStatistics call")
	}
	return r.statistics(ctx, appID, filter)
}
