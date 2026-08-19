package postgres

import (
	"context"
	"testing"
)

func TestOpenWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool, err := Open(ctx, Config{
		Host:     "localhost",
		Port:     5432,
		Database: "app_analytics",
		User:     "app_analytics",
		Password: "app_analytics",
		SSLMode:  "disable",
	})
	if err == nil {
		if pool != nil {
			pool.Close()
		}
		t.Fatal("Open() error = nil, want an error for a canceled context")
	}
	if pool != nil {
		pool.Close()
		t.Error("Open() returned a pool after the connection attempt failed")
	}
}
