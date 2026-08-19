package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"
)

const (
	defaultHTTPPort        = 8080
	defaultShutdownTimeout = 10 * time.Second
)

// Config contains the runtime settings for the API server.
type Config struct {
	HTTPPort        int
	ShutdownTimeout time.Duration
	LogLevel        slog.Level
}

// Load reads configuration from the environment and applies safe local defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPPort:        defaultHTTPPort,
		ShutdownTimeout: defaultShutdownTimeout,
		LogLevel:        slog.LevelInfo,
	}

	if value := os.Getenv("HTTP_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("HTTP_PORT must be an integer between 1 and 65535")
		}
		cfg.HTTPPort = port
	}

	if value := os.Getenv("SHUTDOWN_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration: %q", value)
		}
		cfg.ShutdownTimeout = timeout
	}

	if value := os.Getenv("LOG_LEVEL"); value != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(value)); err != nil {
			return Config{}, fmt.Errorf("LOG_LEVEL must be one of DEBUG, INFO, WARN or ERROR: %w", err)
		}
		cfg.LogLevel = level
	}

	return cfg, nil
}
