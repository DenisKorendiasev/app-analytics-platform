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
	"syscall"

	"github.com/DenisKorendiasev/app-analytics-platform/internal/generator"
)

const (
	defaultAPIURL     = "http://localhost:8080"
	defaultAppCount   = 10
	defaultEventCount = 1000
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("generator", flag.ContinueOnError)
	flags.SetOutput(stderr)
	apiURL := flags.String("api-url", defaultAPIURL, "base URL of the App Analytics API")
	appCount := flags.Int("apps", defaultAppCount, "number of applications to create")
	eventCount := flags.Int("events", defaultEventCount, "number of events to publish")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "generator: unexpected positional arguments: %v\n", flags.Args())
		return 2
	}

	syntheticGenerator, err := generator.New(generator.Config{
		APIURL:     *apiURL,
		AppCount:   *appCount,
		EventCount: *eventCount,
	})
	if err != nil {
		fmt.Fprintf(stderr, "generator: configure: %v\n", err)
		return 1
	}
	summary, err := syntheticGenerator.Run(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "generator: %v\n", err)
		return 1
	}
	if err := json.NewEncoder(stdout).Encode(summary); err != nil {
		fmt.Fprintf(stderr, "generator: write summary: %v\n", err)
		return 1
	}
	return 0
}
