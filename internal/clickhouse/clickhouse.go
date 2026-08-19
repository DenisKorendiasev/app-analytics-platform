// Package clickhouse creates the analytical store connection and repositories.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const connectionTimeout = 5 * time.Second

// Config contains ClickHouse connection settings.
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// Open creates a native ClickHouse connection pool and verifies connectivity.
func Open(ctx context.Context, cfg Config) (driver.Conn, error) {
	connection, err := clickhousego.Open(&clickhousego.Options{
		Addr: []string{net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))},
		Auth: clickhousego.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		DialTimeout:     connectionTimeout,
		MaxOpenConns:    5,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("create ClickHouse connection: %w", err)
	}

	pingContext, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()
	if err := connection.Ping(pingContext); err != nil {
		return nil, errors.Join(
			fmt.Errorf("ping ClickHouse: %w", err),
			closeConnection(connection),
		)
	}
	return connection, nil
}

func closeConnection(connection driver.Conn) error {
	if err := connection.Close(); err != nil {
		return fmt.Errorf("close ClickHouse connection: %w", err)
	}
	return nil
}
