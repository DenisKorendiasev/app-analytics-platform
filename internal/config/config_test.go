package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	clearEnvironment(t)

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
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Postgres.Host = %q, want localhost", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 5432 {
		t.Errorf("Postgres.Port = %d, want 5432", cfg.Postgres.Port)
	}
	if cfg.Postgres.Database != "app_analytics" {
		t.Errorf("Postgres.Database = %q, want app_analytics", cfg.Postgres.Database)
	}
	if cfg.Postgres.User != "app_analytics" {
		t.Errorf("Postgres.User = %q, want app_analytics", cfg.Postgres.User)
	}
	if cfg.Postgres.Password != "app_analytics" {
		t.Error("Postgres.Password does not match the local development default")
	}
	if cfg.Postgres.SSLMode != "disable" {
		t.Errorf("Postgres.SSLMode = %q, want disable", cfg.Postgres.SSLMode)
	}
	if cfg.RabbitMQ.URL != "amqp://app_analytics:app_analytics@localhost:5672/" {
		t.Errorf("RabbitMQ.URL = %q, want local development URL", cfg.RabbitMQ.URL)
	}
	if cfg.RabbitMQ.Exchange != "app.events" {
		t.Errorf("RabbitMQ.Exchange = %q, want app.events", cfg.RabbitMQ.Exchange)
	}
	if cfg.RabbitMQ.Queue != "app.events" {
		t.Errorf("RabbitMQ.Queue = %q, want app.events", cfg.RabbitMQ.Queue)
	}
	if cfg.RabbitMQ.RoutingKey != "app.events" {
		t.Errorf("RabbitMQ.RoutingKey = %q, want app.events", cfg.RabbitMQ.RoutingKey)
	}
	if cfg.ClickHouse.Host != "localhost" {
		t.Errorf("ClickHouse.Host = %q, want localhost", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9000 {
		t.Errorf("ClickHouse.Port = %d, want 9000", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Database != "app_analytics" {
		t.Errorf("ClickHouse.Database = %q, want app_analytics", cfg.ClickHouse.Database)
	}
	if cfg.ClickHouse.User != "app_analytics" {
		t.Errorf("ClickHouse.User = %q, want app_analytics", cfg.ClickHouse.User)
	}
	if cfg.ClickHouse.Password != "app_analytics" {
		t.Errorf("ClickHouse.Password = %q, want local development password", cfg.ClickHouse.Password)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("HTTP_PORT", "9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("POSTGRES_HOST", "postgres.internal")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_DB", "analytics_test")
	t.Setenv("POSTGRES_USER", "test_user")
	t.Setenv("POSTGRES_PASSWORD", "test_password")
	t.Setenv("POSTGRES_SSLMODE", "REQUIRE")
	t.Setenv("RABBITMQ_URL", "amqps://user:password@rabbitmq.internal:5671/analytics")
	t.Setenv("RABBITMQ_EXCHANGE", "events.exchange")
	t.Setenv("RABBITMQ_QUEUE", "events.queue")
	t.Setenv("RABBITMQ_ROUTING_KEY", "events.created")
	t.Setenv("CLICKHOUSE_HOST", "clickhouse.internal")
	t.Setenv("CLICKHOUSE_PORT", "9440")
	t.Setenv("CLICKHOUSE_DATABASE", "analytics")
	t.Setenv("CLICKHOUSE_USER", "analytics_user")
	t.Setenv("CLICKHOUSE_PASSWORD", "analytics_password")

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
	if cfg.Postgres.Host != "postgres.internal" {
		t.Errorf("Postgres.Host = %q, want postgres.internal", cfg.Postgres.Host)
	}
	if cfg.Postgres.Port != 5433 {
		t.Errorf("Postgres.Port = %d, want 5433", cfg.Postgres.Port)
	}
	if cfg.Postgres.Database != "analytics_test" {
		t.Errorf("Postgres.Database = %q, want analytics_test", cfg.Postgres.Database)
	}
	if cfg.Postgres.User != "test_user" {
		t.Errorf("Postgres.User = %q, want test_user", cfg.Postgres.User)
	}
	if cfg.Postgres.Password != "test_password" {
		t.Error("Postgres.Password does not match the environment value")
	}
	if cfg.Postgres.SSLMode != "require" {
		t.Errorf("Postgres.SSLMode = %q, want require", cfg.Postgres.SSLMode)
	}
	if cfg.RabbitMQ.URL != "amqps://user:password@rabbitmq.internal:5671/analytics" {
		t.Errorf("RabbitMQ.URL = %q, want environment value", cfg.RabbitMQ.URL)
	}
	if cfg.RabbitMQ.Exchange != "events.exchange" {
		t.Errorf("RabbitMQ.Exchange = %q, want events.exchange", cfg.RabbitMQ.Exchange)
	}
	if cfg.RabbitMQ.Queue != "events.queue" {
		t.Errorf("RabbitMQ.Queue = %q, want events.queue", cfg.RabbitMQ.Queue)
	}
	if cfg.RabbitMQ.RoutingKey != "events.created" {
		t.Errorf("RabbitMQ.RoutingKey = %q, want events.created", cfg.RabbitMQ.RoutingKey)
	}
	if cfg.ClickHouse.Host != "clickhouse.internal" {
		t.Errorf("ClickHouse.Host = %q, want clickhouse.internal", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9440 {
		t.Errorf("ClickHouse.Port = %d, want 9440", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Database != "analytics" {
		t.Errorf("ClickHouse.Database = %q, want analytics", cfg.ClickHouse.Database)
	}
	if cfg.ClickHouse.User != "analytics_user" {
		t.Errorf("ClickHouse.User = %q, want analytics_user", cfg.ClickHouse.User)
	}
	if cfg.ClickHouse.Password != "analytics_password" {
		t.Errorf("ClickHouse.Password = %q, want environment value", cfg.ClickHouse.Password)
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
		{name: "non-numeric postgres port", key: "POSTGRES_PORT", value: "postgres"},
		{name: "postgres port out of range", key: "POSTGRES_PORT", value: "65536"},
		{name: "invalid postgres ssl mode", key: "POSTGRES_SSLMODE", value: "sometimes"},
		{name: "invalid rabbitmq URL", key: "RABBITMQ_URL", value: "not-a-url"},
		{name: "invalid rabbitmq scheme", key: "RABBITMQ_URL", value: "https://rabbitmq.internal"},
		{name: "missing rabbitmq host", key: "RABBITMQ_URL", value: "amqp://:5672/"},
		{name: "non-numeric clickhouse port", key: "CLICKHOUSE_PORT", value: "native"},
		{name: "clickhouse port out of range", key: "CLICKHOUSE_PORT", value: "65536"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(tt.key, tt.value)

			if _, err := Load(); err == nil {
				t.Fatal("Load() error = nil, want an error")
			}
		})
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"HTTP_PORT",
		"SHUTDOWN_TIMEOUT",
		"LOG_LEVEL",
		"POSTGRES_HOST",
		"POSTGRES_PORT",
		"POSTGRES_DB",
		"POSTGRES_USER",
		"POSTGRES_PASSWORD",
		"POSTGRES_SSLMODE",
		"RABBITMQ_URL",
		"RABBITMQ_EXCHANGE",
		"RABBITMQ_QUEUE",
		"RABBITMQ_ROUTING_KEY",
		"CLICKHOUSE_HOST",
		"CLICKHOUSE_PORT",
		"CLICKHOUSE_DATABASE",
		"CLICKHOUSE_USER",
		"CLICKHOUSE_PASSWORD",
	} {
		t.Setenv(key, "")
	}
}
