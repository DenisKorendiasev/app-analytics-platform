package generator

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

func TestGeneratorRun(t *testing.T) {
	var (
		mu             sync.Mutex
		applications   []createAppRequest
		acceptedEvents []ingestEventRequest
		applicationIDs []uuid.UUID
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Accept") != "application/json" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("JSON headers = Accept %q, Content-Type %q", r.Header.Get("Accept"), r.Header.Get("Content-Type"))
		}
		switch r.URL.Path {
		case "/api/v1/apps":
			var application createAppRequest
			if err := json.NewDecoder(r.Body).Decode(&application); err != nil {
				t.Errorf("decode application request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			applicationID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(application.Name))
			mu.Lock()
			applications = append(applications, application)
			applicationIDs = append(applicationIDs, applicationID)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			if err := json.NewEncoder(w).Encode(createAppResponse{ID: applicationID}); err != nil {
				t.Errorf("encode application response: %v", err)
			}
		case "/api/v1/events":
			var applicationEvent ingestEventRequest
			if err := json.NewDecoder(r.Body).Decode(&applicationEvent); err != nil {
				t.Errorf("decode event request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			acceptedEvents = append(acceptedEvents, applicationEvent)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	syntheticGenerator, err := New(Config{
		APIURL:     server.URL + "/",
		AppCount:   2,
		EventCount: 6,
		HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	syntheticGenerator.random = rand.New(rand.NewSource(42))
	fixedTime := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	syntheticGenerator.now = func() time.Time { return fixedTime }

	summary, err := syntheticGenerator.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary != (Summary{AppsCreated: 2, EventsAccepted: 6}) {
		t.Errorf("Run() summary = %+v, want 2 apps and 6 events", summary)
	}
	if len(applications) != 2 {
		t.Fatalf("application count = %d, want 2", len(applications))
	}
	if applications[0] != (createAppRequest{Name: "Synthetic App 0001", Publisher: "Synthetic Publisher 01", Category: "games"}) {
		t.Errorf("first application = %+v", applications[0])
	}
	if applications[1] != (createAppRequest{Name: "Synthetic App 0002", Publisher: "Synthetic Publisher 02", Category: "finance"}) {
		t.Errorf("second application = %+v", applications[1])
	}
	if len(acceptedEvents) != 6 {
		t.Fatalf("event count = %d, want 6", len(acceptedEvents))
	}

	typeCounts := make(map[event.Type]int)
	for index, applicationEvent := range acceptedEvents {
		typeCounts[applicationEvent.EventType]++
		if applicationEvent.EventType != eventTypes[index%len(eventTypes)] {
			t.Errorf("event %d type = %q, want %q", index, applicationEvent.EventType, eventTypes[index%len(eventTypes)])
		}
		if !containsUUID(applicationIDs, applicationEvent.AppID) {
			t.Errorf("event %d app ID = %s, want a created application ID", index, applicationEvent.AppID)
		}
		if !containsString(countries, applicationEvent.Country) {
			t.Errorf("event %d country = %q", index, applicationEvent.Country)
		}
		if !containsPlatform(platforms, applicationEvent.Platform) {
			t.Errorf("event %d platform = %q", index, applicationEvent.Platform)
		}
		if applicationEvent.Timestamp.After(fixedTime) || applicationEvent.Timestamp.Before(fixedTime.Add(-timestampWindow)) {
			t.Errorf("event %d timestamp = %s, want within the generation window", index, applicationEvent.Timestamp)
		}
		if !applicationEvent.Timestamp.Equal(applicationEvent.Timestamp.Truncate(time.Millisecond)) {
			t.Errorf("event %d timestamp = %s, want millisecond precision", index, applicationEvent.Timestamp)
		}
		if applicationEvent.EventType == event.TypePurchase {
			if applicationEvent.RevenueCents < 100 || applicationEvent.RevenueCents > 10000 {
				t.Errorf("purchase revenue = %d, want 100..10000", applicationEvent.RevenueCents)
			}
		} else if applicationEvent.RevenueCents != 0 {
			t.Errorf("%s revenue = %d, want 0", applicationEvent.EventType, applicationEvent.RevenueCents)
		}
	}
	for _, eventType := range eventTypes {
		if typeCounts[eventType] != 2 {
			t.Errorf("%s count = %d, want 2", eventType, typeCounts[eventType])
		}
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "relative API URL", cfg: Config{APIURL: "localhost:8080", AppCount: 1, EventCount: 1}, want: "API URL"},
		{name: "unsupported API scheme", cfg: Config{APIURL: "ftp://localhost", AppCount: 1, EventCount: 1}, want: "API URL"},
		{name: "API URL query", cfg: Config{APIURL: "http://localhost?debug=1", AppCount: 1, EventCount: 1}, want: "query or fragment"},
		{name: "zero apps", cfg: Config{APIURL: "http://localhost", EventCount: 1}, want: "app count"},
		{name: "zero events", cfg: Config{APIURL: "http://localhost", AppCount: 1}, want: "event count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("New() error = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestGeneratorAPIErrors(t *testing.T) {
	tests := []struct {
		name        string
		handler     http.Handler
		wantSummary Summary
		wantError   string
	}{
		{
			name: "application rejected",
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
			}),
			wantError: "status 503",
		},
		{
			name: "application response without ID",
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{}`))
			}),
			wantError: "empty id",
		},
		{
			name: "event rejected",
			handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/apps" {
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(createAppResponse{ID: uuid.New()})
					return
				}
				http.Error(w, `{"error":"invalid event"}`, http.StatusUnprocessableEntity)
			}),
			wantSummary: Summary{AppsCreated: 1},
			wantError:   "status 422",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			syntheticGenerator, err := New(Config{APIURL: server.URL, AppCount: 1, EventCount: 1, HTTPClient: server.Client()})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			summary, err := syntheticGenerator.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("Run() error = %v, want error containing %q", err, tt.wantError)
			}
			if summary != tt.wantSummary {
				t.Errorf("Run() summary = %+v, want %+v", summary, tt.wantSummary)
			}
		})
	}
}

func TestGeneratorCanceledContext(t *testing.T) {
	syntheticGenerator, err := New(Config{APIURL: "http://127.0.0.1:1", AppCount: 1, EventCount: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := syntheticGenerator.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Run() error = %v, want context.Canceled", err)
	}
}

func containsUUID(values []uuid.UUID, want uuid.UUID) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsPlatform(values []event.Platform, want event.Platform) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
