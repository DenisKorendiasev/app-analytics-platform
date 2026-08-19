package clickhouse

import (
	"context"
	"testing"
)

func TestOpenWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	connection, err := Open(ctx, Config{
		Host:     "localhost",
		Port:     9000,
		Database: "default",
		User:     "default",
	})
	if connection != nil {
		t.Cleanup(func() {
			if err := connection.Close(); err != nil {
				t.Errorf("close unexpected connection: %v", err)
			}
		})
		t.Error("Open() returned a connection after Ping failed")
	}
	if err == nil {
		t.Fatal("Open() error = nil, want an error for a canceled context")
	}
}
