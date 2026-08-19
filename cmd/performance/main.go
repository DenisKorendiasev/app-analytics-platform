package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/config"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/performance"
)

const (
	defaultEventCount = 1000
	defaultRuns       = 3
	defaultBatches    = "1,100,500,1000"
)

type measureFunc func(context.Context, clickhouseinfra.Config, performance.Options) (performance.Report, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, performance.Measure))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, measure measureFunc) int {
	flags := flag.NewFlagSet("performance", flag.ContinueOnError)
	flags.SetOutput(stderr)
	eventCount := flags.Int("events", defaultEventCount, "number of events in every measurement run")
	runs := flags.Int("runs", defaultRuns, "number of runs per batch size")
	batches := flags.String("batches", defaultBatches, "comma-separated ClickHouse batch sizes")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "performance: unexpected positional arguments: %v\n", flags.Args())
		return 2
	}

	batchSizes, err := parseBatchSizes(*batches)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "performance: configure batches: %v\n", err)
		return 1
	}
	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "performance: load configuration: %v\n", err)
		return 1
	}
	report, err := measure(ctx, clickhouseinfra.Config{
		Host:     cfg.ClickHouse.Host,
		Port:     cfg.ClickHouse.Port,
		Database: cfg.ClickHouse.Database,
		User:     cfg.ClickHouse.User,
		Password: cfg.ClickHouse.Password,
	}, performance.Options{
		EventCount: *eventCount,
		Runs:       *runs,
		BatchSizes: batchSizes,
		Progress: func(progress performance.Progress) {
			_, _ = fmt.Fprintf(stderr, "performance: batch=%d run=%d/%d\n", progress.BatchSize, progress.Run, progress.TotalRuns)
		},
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "performance: %v\n", err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		_, _ = fmt.Fprintf(stderr, "performance: write report: %v\n", err)
		return 1
	}
	return 0
}

func parseBatchSizes(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	batchSizes := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, errors.New("batch size must not be empty")
		}
		batchSize, err := strconv.Atoi(part)
		if err != nil || batchSize <= 0 {
			return nil, fmt.Errorf("batch size %q must be a positive integer", part)
		}
		batchSizes = append(batchSizes, batchSize)
	}
	return batchSizes, nil
}
