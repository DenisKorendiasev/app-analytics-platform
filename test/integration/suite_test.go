//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	postgresinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/postgres"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

const (
	containerStartupTimeout = 5 * time.Minute
	testTimeout             = 30 * time.Second

	testDatabase = "app_analytics_test"
	testUser     = "app_analytics"
	testPassword = "app_analytics"
)

var suiteEnvironment struct {
	postgres   postgresinfra.Config
	rabbitURL  string
	clickhouse clickhouseinfra.Config
}

func TestMain(m *testing.M) {
	os.Exit(runIntegrationSuite(m))
}

func runIntegrationSuite(m *testing.M) (exitCode int) {
	ctx, cancel := context.WithTimeout(context.Background(), containerStartupTimeout)
	defer cancel()

	containers := make([]testcontainers.Container, 0, 3)
	defer func() {
		for index := len(containers) - 1; index >= 0; index-- {
			if err := testcontainers.TerminateContainer(containers[index]); err != nil {
				fmt.Fprintf(os.Stderr, "terminate integration container: %v\n", err)
				exitCode = 1
			}
		}
	}()

	postgresContainer, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase(testDatabase),
		tcpostgres.WithUsername(testUser),
		tcpostgres.WithPassword(testPassword),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start PostgreSQL testcontainer: %v\n", err)
		return 1
	}
	containers = append(containers, postgresContainer)

	postgresURL, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "get PostgreSQL testcontainer URL: %v\n", err)
		return 1
	}
	suiteEnvironment.postgres, err = postgresConfigFromURL(postgresURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse PostgreSQL testcontainer URL: %v\n", err)
		return 1
	}

	rabbitContainer, err := tcrabbitmq.Run(ctx,
		"rabbitmq:4-management-alpine",
		tcrabbitmq.WithAdminUsername(testUser),
		tcrabbitmq.WithAdminPassword(testPassword),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start RabbitMQ testcontainer: %v\n", err)
		return 1
	}
	containers = append(containers, rabbitContainer)

	suiteEnvironment.rabbitURL, err = rabbitContainer.AmqpURL(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get RabbitMQ testcontainer URL: %v\n", err)
		return 1
	}

	clickHouseContainer, err := tcclickhouse.Run(ctx,
		"clickhouse/clickhouse-server:26.3-alpine",
		tcclickhouse.WithDatabase(testDatabase),
		tcclickhouse.WithUsername(testUser),
		tcclickhouse.WithPassword(testPassword),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start ClickHouse testcontainer: %v\n", err)
		return 1
	}
	containers = append(containers, clickHouseContainer)

	clickHouseHost, err := clickHouseContainer.ConnectionHost(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get ClickHouse testcontainer address: %v\n", err)
		return 1
	}
	suiteEnvironment.clickhouse, err = clickHouseConfigFromHost(clickHouseHost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse ClickHouse testcontainer address: %v\n", err)
		return 1
	}

	return m.Run()
}

func postgresConfigFromURL(connectionURL string) (postgresinfra.Config, error) {
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		return postgresinfra.Config{}, err
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		return postgresinfra.Config{}, fmt.Errorf("parse port %q: %w", parsed.Port(), err)
	}
	password, _ := parsed.User.Password()
	return postgresinfra.Config{
		Host:     parsed.Hostname(),
		Port:     port,
		Database: testDatabase,
		User:     parsed.User.Username(),
		Password: password,
		SSLMode:  "disable",
	}, nil
}

func clickHouseConfigFromHost(connectionHost string) (clickhouseinfra.Config, error) {
	host, portText, err := net.SplitHostPort(connectionHost)
	if err != nil {
		return clickhouseinfra.Config{}, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return clickhouseinfra.Config{}, fmt.Errorf("parse port %q: %w", portText, err)
	}
	return clickhouseinfra.Config{
		Host:     host,
		Port:     port,
		Database: testDatabase,
		User:     testUser,
		Password: testPassword,
	}, nil
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}
