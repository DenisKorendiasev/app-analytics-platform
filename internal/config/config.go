package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPPort           = 8080
	defaultShutdownTimeout    = 10 * time.Second
	defaultPostgresHost       = "localhost"
	defaultPostgresPort       = 5432
	defaultPostgresDatabase   = "app_analytics"
	defaultPostgresUser       = "app_analytics"
	defaultPostgresPassword   = "app_analytics"
	defaultPostgresSSLMode    = "disable"
	defaultRabbitMQURL        = "amqp://app_analytics:app_analytics@localhost:5672/"
	defaultRabbitMQExchange   = "app.events"
	defaultRabbitMQQueue      = "app.events"
	defaultRabbitMQRoutingKey = "app.events"
	defaultClickHouseHost     = "localhost"
	defaultClickHousePort     = 9000
	defaultClickHouseDatabase = "app_analytics"
	defaultClickHouseUser     = "app_analytics"
	defaultClickHousePassword = "app_analytics"
)

// Config contains runtime settings shared by the platform applications.
type Config struct {
	HTTPPort        int
	ShutdownTimeout time.Duration
	LogLevel        slog.Level
	Postgres        PostgresConfig
	RabbitMQ        RabbitMQConfig
	ClickHouse      ClickHouseConfig
}

// ClickHouseConfig contains the settings required to connect to ClickHouse.
type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// RabbitMQConfig contains the settings required to connect to RabbitMQ.
type RabbitMQConfig struct {
	URL        string
	Exchange   string
	Queue      string
	RoutingKey string
}

// PostgresConfig contains the settings required to connect to PostgreSQL.
type PostgresConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	SSLMode  string
}

// Load reads configuration from the environment and applies safe local defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPPort:        defaultHTTPPort,
		ShutdownTimeout: defaultShutdownTimeout,
		LogLevel:        slog.LevelInfo,
		Postgres: PostgresConfig{
			Host:     defaultPostgresHost,
			Port:     defaultPostgresPort,
			Database: defaultPostgresDatabase,
			User:     defaultPostgresUser,
			Password: defaultPostgresPassword,
			SSLMode:  defaultPostgresSSLMode,
		},
		RabbitMQ: RabbitMQConfig{
			URL:        defaultRabbitMQURL,
			Exchange:   defaultRabbitMQExchange,
			Queue:      defaultRabbitMQQueue,
			RoutingKey: defaultRabbitMQRoutingKey,
		},
		ClickHouse: ClickHouseConfig{
			Host:     defaultClickHouseHost,
			Port:     defaultClickHousePort,
			Database: defaultClickHouseDatabase,
			User:     defaultClickHouseUser,
			Password: defaultClickHousePassword,
		},
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

	if value := os.Getenv("POSTGRES_HOST"); value != "" {
		cfg.Postgres.Host = value
	}
	if value := os.Getenv("POSTGRES_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("POSTGRES_PORT must be an integer between 1 and 65535")
		}
		cfg.Postgres.Port = port
	}
	if value := os.Getenv("POSTGRES_DB"); value != "" {
		cfg.Postgres.Database = value
	}
	if value := os.Getenv("POSTGRES_USER"); value != "" {
		cfg.Postgres.User = value
	}
	if value := os.Getenv("POSTGRES_PASSWORD"); value != "" {
		cfg.Postgres.Password = value
	}
	if value := os.Getenv("POSTGRES_SSLMODE"); value != "" {
		sslMode := strings.ToLower(value)
		if !validSSLMode(sslMode) {
			return Config{}, fmt.Errorf("POSTGRES_SSLMODE must be one of disable, allow, prefer, require, verify-ca or verify-full")
		}
		cfg.Postgres.SSLMode = sslMode
	}

	if value := os.Getenv("RABBITMQ_URL"); value != "" {
		parsedURL, err := url.Parse(value)
		if err != nil || (parsedURL.Scheme != "amqp" && parsedURL.Scheme != "amqps") || parsedURL.Hostname() == "" {
			return Config{}, fmt.Errorf("RABBITMQ_URL must be a valid amqp or amqps URL")
		}
		cfg.RabbitMQ.URL = value
	}
	if value := os.Getenv("RABBITMQ_EXCHANGE"); value != "" {
		cfg.RabbitMQ.Exchange = value
	}
	if value := os.Getenv("RABBITMQ_QUEUE"); value != "" {
		cfg.RabbitMQ.Queue = value
	}
	if value := os.Getenv("RABBITMQ_ROUTING_KEY"); value != "" {
		cfg.RabbitMQ.RoutingKey = value
	}

	if value := os.Getenv("CLICKHOUSE_HOST"); value != "" {
		cfg.ClickHouse.Host = value
	}
	if value := os.Getenv("CLICKHOUSE_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("CLICKHOUSE_PORT must be an integer between 1 and 65535")
		}
		cfg.ClickHouse.Port = port
	}
	if value := os.Getenv("CLICKHOUSE_DATABASE"); value != "" {
		cfg.ClickHouse.Database = value
	}
	if value := os.Getenv("CLICKHOUSE_USER"); value != "" {
		cfg.ClickHouse.User = value
	}
	if value := os.Getenv("CLICKHOUSE_PASSWORD"); value != "" {
		cfg.ClickHouse.Password = value
	}

	return cfg, nil
}

func validSSLMode(value string) bool {
	switch value {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}
