//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/analytics"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/app"
	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	postgresinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/rabbitmq"
	workerapp "github.com/DenisKorendiasev/app-analytics-platform/internal/worker"
	"github.com/google/uuid"
)

func TestEndToEndEventAnalytics(t *testing.T) {
	ctx := integrationContext(t)
	postgresPool := newPostgresPool(t, ctx)
	clickHouseConnection := newClickHouseConnection(t, ctx)
	rabbitConfiguration := newRabbitConfig()

	publisher, err := rabbitmq.NewPublisher(ctx, rabbitConfiguration)
	if err != nil {
		t.Fatalf("create RabbitMQ publisher: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close RabbitMQ publisher: %v", err)
		}
	})
	consumer, err := rabbitmq.NewConsumer(ctx, rabbitConfiguration)
	if err != nil {
		t.Fatalf("create RabbitMQ consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close RabbitMQ consumer: %v", err)
		}
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	appRepository := postgresinfra.NewAppRepository(postgresPool)
	statisticsRepository := clickhouseinfra.NewStatisticsRepository(clickHouseConnection)
	mux := http.NewServeMux()
	app.NewHandler(app.NewService(appRepository), logger).RegisterRoutes(mux)
	event.NewHandler(event.NewService(appRepository, publisher), logger).RegisterRoutes(mux)
	analytics.NewHandler(analytics.NewService(statisticsRepository), logger).RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	workerContext, stopWorker := context.WithCancel(ctx)
	workerResult := make(chan error, 1)
	go func() {
		worker := workerapp.New(consumer, clickhouseinfra.NewEventRepository(clickHouseConnection), logger)
		workerResult <- worker.Run(workerContext)
	}()
	workerStopped := false
	t.Cleanup(func() {
		stopWorker()
		if workerStopped {
			return
		}
		select {
		case err := <-workerResult:
			if err != nil {
				t.Errorf("Worker cleanup error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Worker did not stop during cleanup")
		}
	})

	appID := createApplicationOverHTTP(t, ctx, server.URL)
	publishEventOverHTTP(t, ctx, server.URL, appID)
	assertStatisticsOverHTTP(t, ctx, server.URL, appID, workerResult)

	stopWorker()
	select {
	case err := <-workerResult:
		workerStopped = true
		if err != nil {
			t.Fatalf("Worker Run() error = %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for Worker shutdown: %v", ctx.Err())
	}
}

func createApplicationOverHTTP(t *testing.T, ctx context.Context, serverURL string) uuid.UUID {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/api/v1/apps", bytes.NewBufferString(
		`{"name":"Integration App","publisher":"Test Publisher","category":"test"}`,
	))
	if err != nil {
		t.Fatalf("create application request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("create application over HTTP: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create application status = %d, want %d; body = %s", response.StatusCode, http.StatusCreated, body)
	}
	var application struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&application); err != nil {
		t.Fatalf("decode created application: %v", err)
	}
	if application.ID == uuid.Nil {
		t.Fatal("created application ID is empty")
	}
	return application.ID
}

func publishEventOverHTTP(t *testing.T, ctx context.Context, serverURL string, appID uuid.UUID) {
	t.Helper()
	payload := fmt.Sprintf(
		`{"app_id":%q,"event_type":"purchase","country":"RS","platform":"android","revenue_cents":1299,"timestamp":"2026-08-19T14:13:30Z"}`,
		appID,
	)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/api/v1/events", bytes.NewBufferString(payload))
	if err != nil {
		t.Fatalf("create event request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("publish event over HTTP: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("publish event status = %d, want %d; body = %s", response.StatusCode, http.StatusAccepted, body)
	}
}

func assertStatisticsOverHTTP(t *testing.T, ctx context.Context, serverURL string, appID uuid.UUID, workerResult <-chan error) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		statistics, err := getStatistics(ctx, serverURL, appID)
		if err == nil && statistics == (analytics.Statistics{
			AppID:        appID,
			Purchases:    1,
			RevenueCents: 1299,
		}) {
			return
		}
		select {
		case workerErr := <-workerResult:
			t.Fatalf("Worker stopped before statistics became available: %v", workerErr)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wait for event statistics: %v; last response = %+v; last error = %v", ctx.Err(), statistics, err)
		}
	}
}

func getStatistics(ctx context.Context, serverURL string, appID uuid.UUID) (analytics.Statistics, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL+"/api/v1/apps/"+appID.String()+"/stats", nil)
	if err != nil {
		return analytics.Statistics{}, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return analytics.Statistics{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return analytics.Statistics{}, fmt.Errorf("statistics status %d: %s", response.StatusCode, body)
	}
	var statistics struct {
		AppID        uuid.UUID `json:"app_id"`
		Installs     uint64    `json:"installs"`
		Sessions     uint64    `json:"sessions"`
		Purchases    uint64    `json:"purchases"`
		RevenueCents int64     `json:"revenue_cents"`
	}
	if err := json.NewDecoder(response.Body).Decode(&statistics); err != nil {
		return analytics.Statistics{}, err
	}
	return analytics.Statistics{
		AppID:        statistics.AppID,
		Installs:     statistics.Installs,
		Sessions:     statistics.Sessions,
		Purchases:    statistics.Purchases,
		RevenueCents: statistics.RevenueCents,
	}, nil
}
