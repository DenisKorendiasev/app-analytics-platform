// Package generator creates synthetic application data through the public API.
package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/event"
	"github.com/google/uuid"
)

const (
	defaultRequestTimeout = 10 * time.Second
	timestampWindow       = 30 * 24 * time.Hour
	maxErrorBodySize      = 4 << 10
)

var (
	eventTypes = []event.Type{
		event.TypeInstall,
		event.TypeSession,
		event.TypePurchase,
	}
	countries  = []string{"US", "GB", "DE", "FR", "BR", "IN", "JP", "RS"}
	platforms  = []event.Platform{event.PlatformAndroid, event.PlatformIOS}
	categories = []string{"games", "finance", "health", "music", "productivity"}
)

// Config controls one generator run.
type Config struct {
	APIURL     string
	AppCount   int
	EventCount int
	HTTPClient *http.Client
}

// Summary reports how many resources were accepted by the API.
type Summary struct {
	AppsCreated    int `json:"apps_created"`
	EventsAccepted int `json:"events_accepted"`
}

// Generator sends synthetic applications and events to the platform API.
type Generator struct {
	baseURL    string
	appCount   int
	eventCount int
	client     *http.Client
	random     *rand.Rand
	now        func() time.Time
}

// New validates configuration and creates a synthetic data Generator.
func New(cfg Config) (*Generator, error) {
	baseURL, err := validateAPIURL(cfg.APIURL)
	if err != nil {
		return nil, err
	}
	if cfg.AppCount <= 0 {
		return nil, errors.New("app count must be greater than zero")
	}
	if cfg.EventCount <= 0 {
		return nil, errors.New("event count must be greater than zero")
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Generator{
		baseURL:    baseURL,
		appCount:   cfg.AppCount,
		eventCount: cfg.EventCount,
		client:     client,
		random:     rand.New(rand.NewSource(time.Now().UnixNano())),
		now:        time.Now,
	}, nil
}

// Run creates applications first and then publishes synthetic events for them.
func (g *Generator) Run(ctx context.Context) (Summary, error) {
	applicationIDs := make([]uuid.UUID, 0, g.appCount)
	summary := Summary{}
	for index := 0; index < g.appCount; index++ {
		applicationID, err := g.createApplication(ctx, index)
		if err != nil {
			return summary, fmt.Errorf("create synthetic app %d of %d: %w", index+1, g.appCount, err)
		}
		applicationIDs = append(applicationIDs, applicationID)
		summary.AppsCreated++
	}

	generationTime := g.now().UTC()
	for index := 0; index < g.eventCount; index++ {
		applicationEvent := g.newEvent(index, applicationIDs, generationTime)
		if err := g.publishEvent(ctx, applicationEvent); err != nil {
			return summary, fmt.Errorf("publish synthetic event %d of %d: %w", index+1, g.eventCount, err)
		}
		summary.EventsAccepted++
	}
	return summary, nil
}

func (g *Generator) createApplication(ctx context.Context, index int) (uuid.UUID, error) {
	requestBody := createAppRequest{
		Name:      fmt.Sprintf("Synthetic App %04d", index+1),
		Publisher: fmt.Sprintf("Synthetic Publisher %02d", index%10+1),
		Category:  categories[index%len(categories)],
	}
	response, err := g.postJSON(ctx, "/api/v1/apps", requestBody)
	if err != nil {
		return uuid.Nil, err
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusCreated {
		return uuid.Nil, unexpectedStatus(response)
	}

	var application createAppResponse
	if err := json.NewDecoder(response.Body).Decode(&application); err != nil {
		return uuid.Nil, fmt.Errorf("decode create app response: %w", err)
	}
	if application.ID == uuid.Nil {
		return uuid.Nil, errors.New("create app response contains an empty id")
	}
	return application.ID, nil
}

func (g *Generator) newEvent(index int, applicationIDs []uuid.UUID, generationTime time.Time) ingestEventRequest {
	eventType := eventTypes[index%len(eventTypes)]
	revenueCents := int64(0)
	if eventType == event.TypePurchase {
		revenueCents = 100 + g.random.Int63n(9901)
	}
	return ingestEventRequest{
		AppID:        applicationIDs[g.random.Intn(len(applicationIDs))],
		EventType:    eventType,
		Country:      countries[g.random.Intn(len(countries))],
		Platform:     platforms[g.random.Intn(len(platforms))],
		RevenueCents: revenueCents,
		Timestamp:    generationTime.Add(-time.Duration(g.random.Int63n(int64(timestampWindow)))).Truncate(time.Millisecond),
	}
}

func (g *Generator) publishEvent(ctx context.Context, applicationEvent ingestEventRequest) error {
	response, err := g.postJSON(ctx, "/api/v1/events", applicationEvent)
	if err != nil {
		return err
	}
	defer drainAndClose(response.Body)
	if response.StatusCode != http.StatusAccepted {
		return unexpectedStatus(response)
	}
	return nil
}

func (g *Generator) postJSON(ctx context.Context, path string, value any) (*http.Response, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode HTTP request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	response, err := g.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("send HTTP request: %w", err)
	}
	return response, nil
}

func validateAPIURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("API URL must be an absolute http or https URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("API URL must not contain a query or fragment")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func unexpectedStatus(response *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBodySize))
	if err != nil {
		return fmt.Errorf("API returned status %d and its response body could not be read: %w", response.StatusCode, err)
	}
	message := strings.TrimSpace(string(body))
	if message == "" {
		return fmt.Errorf("API returned status %d", response.StatusCode)
	}
	return fmt.Errorf("API returned status %d: %s", response.StatusCode, message)
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

type createAppRequest struct {
	Name      string `json:"name"`
	Publisher string `json:"publisher"`
	Category  string `json:"category"`
}

type createAppResponse struct {
	ID uuid.UUID `json:"id"`
}

type ingestEventRequest struct {
	AppID        uuid.UUID      `json:"app_id"`
	EventType    event.Type     `json:"event_type"`
	Country      string         `json:"country"`
	Platform     event.Platform `json:"platform"`
	RevenueCents int64          `json:"revenue_cents"`
	Timestamp    time.Time      `json:"timestamp"`
}
