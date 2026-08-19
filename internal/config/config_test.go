package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HTTP_PORT", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")
	t.Setenv("LOG_LEVEL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.HTTPPort)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 10s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %s, want INFO", cfg.LogLevel)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.HTTPPort != 9090 {
		t.Errorf("HTTPPort = %d, want 9090", cfg.HTTPPort)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Errorf("ShutdownTimeout = %s, want 3s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %s, want DEBUG", cfg.LogLevel)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "non-numeric port", key: "HTTP_PORT", value: "http"},
		{name: "port out of range", key: "HTTP_PORT", value: "65536"},
		{name: "invalid timeout", key: "SHUTDOWN_TIMEOUT", value: "soon"},
		{name: "non-positive timeout", key: "SHUTDOWN_TIMEOUT", value: "0s"},
		{name: "invalid log level", key: "LOG_LEVEL", value: "verbose"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HTTP_PORT", "")
			t.Setenv("SHUTDOWN_TIMEOUT", "")
			t.Setenv("LOG_LEVEL", "")
			t.Setenv(tt.key, tt.value)

			if _, err := Load(); err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
		})
	}
}
