package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
)

func TestOpen(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION_TEST") != "1" {
		t.Skip("set POSTGRES_INTEGRATION_TEST=1 to run the PostgreSQL integration test")
	}

	applicationConfig, err := config.Load()
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.Open(ctx, postgres.Config{
		Host:     applicationConfig.Postgres.Host,
		Port:     applicationConfig.Postgres.Port,
		Database: applicationConfig.Postgres.Database,
		User:     applicationConfig.Postgres.User,
		Password: applicationConfig.Postgres.Password,
		SSLMode:  applicationConfig.Postgres.SSLMode,
	})
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer pool.Close()
}
