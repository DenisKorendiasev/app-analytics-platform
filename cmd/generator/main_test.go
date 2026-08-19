package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/generator"
	"github.com/google/uuid"
)

func TestRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/apps":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]uuid.UUID{"id": uuid.New()})
		case "/api/v1/events":
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"--api-url", server.URL,
		"--apps", "2",
		"--events", "3",
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run() exit code = %d, want 0; stderr = %s", exitCode, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
	var summary generator.Summary
	if err := json.NewDecoder(&stdout).Decode(&summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary != (generator.Summary{AppsCreated: 2, EventsAccepted: 3}) {
		t.Errorf("summary = %+v, want 2 apps and 3 events", summary)
	}
}

func TestRunErrors(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantExitCode int
		wantError    string
	}{
		{name: "invalid flag", args: []string{"--apps", "many"}, wantExitCode: 2, wantError: "invalid value"},
		{name: "unexpected argument", args: []string{"extra"}, wantExitCode: 2, wantError: "unexpected positional arguments"},
		{name: "invalid count", args: []string{"--apps", "0"}, wantExitCode: 1, wantError: "app count"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			exitCode := run(context.Background(), tt.args, bytes.NewBuffer(nil), &stderr)
			if exitCode != tt.wantExitCode {
				t.Errorf("run() exit code = %d, want %d", exitCode, tt.wantExitCode)
			}
			if !strings.Contains(stderr.String(), tt.wantError) {
				t.Errorf("stderr = %q, want text containing %q", stderr.String(), tt.wantError)
			}
		})
	}
}

func TestRunHelp(t *testing.T) {
	var stderr bytes.Buffer
	if exitCode := run(context.Background(), []string{"--help"}, bytes.NewBuffer(nil), &stderr); exitCode != 0 {
		t.Errorf("run() help exit code = %d, want 0", exitCode)
	}
	if !strings.Contains(stderr.String(), "Usage of generator") {
		t.Errorf("help output = %q", stderr.String())
	}
}
