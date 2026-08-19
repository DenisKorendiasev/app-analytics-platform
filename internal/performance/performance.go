// Package performance measures ClickHouse event insertion and analytics query performance.
package performance

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

const (
	cleanupTimeout = 30 * time.Second
	warmupEvents   = 1000
	analysisEvents = 100000
	analysisBatch  = 1000
)

var defaultBatchSizes = []int{1, 100, 500, 1000}

// Options controls a performance measurement run.
type Options struct {
	EventCount int
	Runs       int
	BatchSizes []int
	Progress   func(Progress)
}

// Progress identifies the insertion scenario that is about to run.
type Progress struct {
	BatchSize int
	Run       int
	TotalRuns int
}

// Report contains insertion measurements and analytics query analysis.
type Report struct {
	EventCount     int           `json:"event_count"`
	Runs           int           `json:"runs"`
	Measurements   []Measurement `json:"measurements"`
	AnalyticsQuery QueryAnalysis `json:"analytics_query"`
}

// Measurement summarizes repeated runs for one batch size.
type Measurement struct {
	BatchSize                        int              `json:"batch_size"`
	InsertOperations                 int              `json:"insert_operations"`
	MedianEventsPerSecond            float64          `json:"median_events_per_second"`
	MedianProcessingDurationMS       float64          `json:"median_processing_duration_ms"`
	MedianClickHouseInsertDurationMS float64          `json:"median_clickhouse_insert_duration_ms"`
	Runs                             []RunMeasurement `json:"runs"`
}

// RunMeasurement contains the directly observed metrics for one run.
type RunMeasurement struct {
	EventsPerSecond            float64 `json:"events_per_second"`
	ProcessingDurationMS       float64 `json:"processing_duration_ms"`
	ClickHouseInsertDurationMS float64 `json:"clickhouse_insert_duration_ms"`
}

// QueryAnalysis captures a real statistics query result and its ClickHouse plan.
type QueryAnalysis struct {
	DatasetEvents int             `json:"dataset_events"`
	AppID         uuid.UUID       `json:"app_id"`
	DurationMS    float64         `json:"duration_ms"`
	Result        QueryAggregates `json:"result"`
	Explain       []string        `json:"explain"`
}

// QueryAggregates is the JSON representation of an analyzed statistics result.
type QueryAggregates struct {
	Installs     uint64 `json:"installs"`
	Sessions     uint64 `json:"sessions"`
	Purchases    uint64 `json:"purchases"`
	RevenueCents int64  `json:"revenue_cents"`
}

// Measure runs insertion scenarios in a temporary ClickHouse database cloned
// from the configured events table. The source database is never modified.
func Measure(ctx context.Context, cfg clickhouseinfra.Config, options Options) (report Report, resultError error) {
	options, err := validateOptions(options)
	if err != nil {
		return Report{}, err
	}

	adminConfig := cfg
	adminConfig.Database = "default"
	adminConnection, err := clickhouseinfra.Open(ctx, adminConfig)
	if err != nil {
		return Report{}, fmt.Errorf("open ClickHouse admin connection: %w", err)
	}
	defer func() {
		if err := adminConnection.Close(); err != nil {
			resultError = errors.Join(resultError, fmt.Errorf("close ClickHouse admin connection: %w", err))
		}
	}()

	databaseName := "performance_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if err := adminConnection.Exec(ctx, "CREATE DATABASE "+quoteIdentifier(databaseName)); err != nil {
		return Report{}, fmt.Errorf("create temporary performance database: %w", err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := adminConnection.Exec(cleanupContext, "DROP DATABASE IF EXISTS "+quoteIdentifier(databaseName)+" SYNC"); err != nil {
			resultError = errors.Join(resultError, fmt.Errorf("drop temporary performance database: %w", err))
		}
	}()

	cloneQuery := fmt.Sprintf(
		"CREATE TABLE %s.events AS %s.events",
		quoteIdentifier(databaseName),
		quoteIdentifier(cfg.Database),
	)
	if err := adminConnection.Exec(ctx, cloneQuery); err != nil {
		return Report{}, fmt.Errorf("clone events table into temporary performance database: %w", err)
	}

	measurementConfig := cfg
	measurementConfig.Database = databaseName
	connection, err := clickhouseinfra.Open(ctx, measurementConfig)
	if err != nil {
		return Report{}, fmt.Errorf("open temporary performance database: %w", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			resultError = errors.Join(resultError, fmt.Errorf("close temporary performance connection: %w", err))
		}
	}()

	events, _ := generateEvents(options.EventCount)
	repository := clickhouseinfra.NewEventRepository(connection)
	if err := warmUp(ctx, connection, repository, events); err != nil {
		return Report{}, err
	}

	runner := runner{
		writer: repository,
		reset: func(ctx context.Context) error {
			return connection.Exec(ctx, "TRUNCATE TABLE events")
		},
		events:     events,
		batchSizes: options.BatchSizes,
		runs:       options.Runs,
		now:        time.Now,
		progress:   options.Progress,
	}
	measurements, err := runner.run(ctx)
	if err != nil {
		return Report{}, err
	}

	analyzedAppID, err := prepareAnalysisDataset(ctx, connection, repository)
	if err != nil {
		return Report{}, err
	}
	queryAnalysis, err := analyzeStatisticsQuery(ctx, connection, analyzedAppID)
	if err != nil {
		return Report{}, err
	}

	return Report{
		EventCount:     options.EventCount,
		Runs:           options.Runs,
		Measurements:   measurements,
		AnalyticsQuery: queryAnalysis,
	}, nil
}

type eventWriter interface {
	InsertBatch(ctx context.Context, events []event.Event) error
}

type runner struct {
	writer     eventWriter
	reset      func(context.Context) error
	events     []event.Event
	batchSizes []int
	runs       int
	now        func() time.Time
	progress   func(Progress)
}

func (r runner) run(ctx context.Context) ([]Measurement, error) {
	measurements := make([]Measurement, 0, len(r.batchSizes))
	for _, batchSize := range r.batchSizes {
		measurement := Measurement{
			BatchSize:        batchSize,
			InsertOperations: (len(r.events) + batchSize - 1) / batchSize,
			Runs:             make([]RunMeasurement, 0, r.runs),
		}
		for runIndex := 0; runIndex < r.runs; runIndex++ {
			if r.progress != nil {
				r.progress(Progress{BatchSize: batchSize, Run: runIndex + 1, TotalRuns: r.runs})
			}
			if err := r.reset(ctx); err != nil {
				return nil, fmt.Errorf("reset events before batch %d run %d: %w", batchSize, runIndex+1, err)
			}

			processingStart := r.now()
			var insertDuration time.Duration
			for start := 0; start < len(r.events); start += batchSize {
				end := min(start+batchSize, len(r.events))
				insertStart := r.now()
				if err := r.writer.InsertBatch(ctx, r.events[start:end]); err != nil {
					return nil, fmt.Errorf("insert batch %d run %d events %d..%d: %w", batchSize, runIndex+1, start+1, end, err)
				}
				insertDuration += r.now().Sub(insertStart)
			}
			processingDuration := r.now().Sub(processingStart)
			measurement.Runs = append(measurement.Runs, RunMeasurement{
				EventsPerSecond:            float64(len(r.events)) / processingDuration.Seconds(),
				ProcessingDurationMS:       durationMilliseconds(processingDuration),
				ClickHouseInsertDurationMS: durationMilliseconds(insertDuration),
			})
		}
		measurement.MedianEventsPerSecond = median(measurement.Runs, func(run RunMeasurement) float64 { return run.EventsPerSecond })
		measurement.MedianProcessingDurationMS = median(measurement.Runs, func(run RunMeasurement) float64 { return run.ProcessingDurationMS })
		measurement.MedianClickHouseInsertDurationMS = median(measurement.Runs, func(run RunMeasurement) float64 { return run.ClickHouseInsertDurationMS })
		measurements = append(measurements, measurement)
	}
	return measurements, nil
}

func validateOptions(options Options) (Options, error) {
	if options.EventCount <= 0 {
		return Options{}, errors.New("event count must be greater than zero")
	}
	if options.Runs <= 0 {
		return Options{}, errors.New("run count must be greater than zero")
	}
	if len(options.BatchSizes) == 0 {
		options.BatchSizes = append([]int(nil), defaultBatchSizes...)
	}
	seen := make(map[int]struct{}, len(options.BatchSizes))
	for _, batchSize := range options.BatchSizes {
		if batchSize <= 0 {
			return Options{}, errors.New("batch sizes must be greater than zero")
		}
		if _, exists := seen[batchSize]; exists {
			return Options{}, fmt.Errorf("batch size %d is duplicated", batchSize)
		}
		seen[batchSize] = struct{}{}
	}
	options.BatchSizes = append([]int(nil), options.BatchSizes...)
	return options, nil
}

func warmUp(ctx context.Context, connection clickhousego.Conn, writer eventWriter, events []event.Event) error {
	count := min(warmupEvents, len(events))
	if err := writer.InsertBatch(ctx, events[:count]); err != nil {
		return fmt.Errorf("warm up ClickHouse insertion: %w", err)
	}
	if err := connection.Exec(ctx, "TRUNCATE TABLE events"); err != nil {
		return fmt.Errorf("reset events after warm-up: %w", err)
	}
	return nil
}

func prepareAnalysisDataset(ctx context.Context, connection clickhousego.Conn, writer eventWriter) (uuid.UUID, error) {
	if err := connection.Exec(ctx, "TRUNCATE TABLE events"); err != nil {
		return uuid.Nil, fmt.Errorf("reset events before analytics analysis: %w", err)
	}
	events, analyzedAppID := generateEvents(analysisEvents)
	for start := 0; start < len(events); start += analysisBatch {
		end := min(start+analysisBatch, len(events))
		if err := writer.InsertBatch(ctx, events[start:end]); err != nil {
			return uuid.Nil, fmt.Errorf("prepare analytics dataset events %d..%d: %w", start+1, end, err)
		}
	}
	if err := connection.Exec(ctx, "OPTIMIZE TABLE events FINAL"); err != nil {
		return uuid.Nil, fmt.Errorf("optimize events before analytics analysis: %w", err)
	}
	return analyzedAppID, nil
}

func generateEvents(count int) ([]event.Event, uuid.UUID) {
	random := rand.New(rand.NewSource(42))
	applicationIDs := make([]uuid.UUID, 100)
	for index := range applicationIDs {
		applicationIDs[index] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("performance-app-%03d", index)))
	}
	baseTime := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	eventTypes := []event.Type{event.TypeInstall, event.TypeSession, event.TypePurchase}
	countries := []string{"US", "GB", "DE", "FR", "BR", "IN", "JP", "RS"}
	platforms := []event.Platform{event.PlatformAndroid, event.PlatformIOS}

	events := make([]event.Event, count)
	for index := range events {
		eventType := eventTypes[index%len(eventTypes)]
		revenueCents := int64(0)
		if eventType == event.TypePurchase {
			revenueCents = 100 + random.Int63n(9901)
		}
		events[index] = event.Event{
			EventID:      uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("performance-event-%09d", index))),
			AppID:        applicationIDs[index%len(applicationIDs)],
			EventType:    eventType,
			Country:      countries[random.Intn(len(countries))],
			Platform:     platforms[random.Intn(len(platforms))],
			RevenueCents: revenueCents,
			Timestamp:    baseTime.Add(-time.Duration(index%720) * time.Hour),
		}
	}
	return events, applicationIDs[0]
}

func analyzeStatisticsQuery(ctx context.Context, connection clickhousego.Conn, appID uuid.UUID) (QueryAnalysis, error) {
	const query = `
		SELECT
			countIf(event_type = 'install'),
			countIf(event_type = 'session'),
			countIf(event_type = 'purchase'),
			sumIf(revenue_cents, event_type = 'purchase')
		FROM events
		WHERE app_id = ?`

	explainRows, err := connection.Query(ctx, "EXPLAIN indexes = 1 "+query, appID)
	if err != nil {
		return QueryAnalysis{}, fmt.Errorf("explain application statistics query: %w", err)
	}
	defer explainRows.Close()
	plan := make([]string, 0, 16)
	for explainRows.Next() {
		var line string
		if err := explainRows.Scan(&line); err != nil {
			return QueryAnalysis{}, fmt.Errorf("scan application statistics explanation: %w", err)
		}
		plan = append(plan, line)
	}
	if err := explainRows.Err(); err != nil {
		return QueryAnalysis{}, fmt.Errorf("read application statistics explanation: %w", err)
	}

	start := time.Now()
	var aggregates analytics.Aggregates
	if err := connection.QueryRow(ctx, query, appID).Scan(
		&aggregates.Installs,
		&aggregates.Sessions,
		&aggregates.Purchases,
		&aggregates.RevenueCents,
	); err != nil {
		return QueryAnalysis{}, fmt.Errorf("measure application statistics query: %w", err)
	}

	return QueryAnalysis{
		DatasetEvents: analysisEvents,
		AppID:         appID,
		DurationMS:    durationMilliseconds(time.Since(start)),
		Result: QueryAggregates{
			Installs:     aggregates.Installs,
			Sessions:     aggregates.Sessions,
			Purchases:    aggregates.Purchases,
			RevenueCents: aggregates.RevenueCents,
		},
		Explain: plan,
	}, nil
}

func durationMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func median[T any](values []T, selectValue func(T) float64) float64 {
	numbers := make([]float64, len(values))
	for index, value := range values {
		numbers[index] = selectValue(value)
	}
	sort.Float64s(numbers)
	middle := len(numbers) / 2
	if len(numbers)%2 == 1 {
		return numbers[middle]
	}
	return (numbers[middle-1] + numbers[middle]) / 2
}

func quoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
