package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	clickhouseinfra "github.com/DenisKorendiasev/app-analytics-platform/internal/clickhouse"
	"github.com/DenisKorendiasev/app-analytics-platform/internal/performance"
)

func TestRun(t *testing.T) {
	var capturedOptions performance.Options
	measure := func(_ context.Context, _ clickhouseinfra.Config, options performance.Options) (performance.Report, error) {
		capturedOptions = options
		options.Progress(performance.Progress{BatchSize: 5, Run: 1, TotalRuns: 2})
		return performance.Report{EventCount: options.EventCount, Runs: options.Runs}, nil
	}
	var stdout, stderr bytes.Buffer
	exitCode := run(context.Background(), []string{"--events=25", "--runs=2", "--batches=1,5,10"}, &stdout, &stderr, measure)
	if exitCode != 0 {
		t.Fatalf("run() exit code = %d, stderr = %s", exitCode, stderr.String())
	}
	if capturedOptions.EventCount != 25 || capturedOptions.Runs != 2 {
		t.Errorf("captured options = %+v", capturedOptions)
	}
	if !strings.Contains(stderr.String(), "batch=5 run=1/2") {
		t.Errorf("progress output = %q", stderr.String())
	}
	wantBatches := []int{1, 5, 10}
	for index, want := range wantBatches {
		if capturedOptions.BatchSizes[index] != want {
			t.Errorf("batch size %d = %d, want %d", index, capturedOptions.BatchSizes[index], want)
		}
	}
	var report performance.Report
	if err := json.NewDecoder(&stdout).Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.EventCount != 25 || report.Runs != 2 {
		t.Errorf("report = %+v", report)
	}
}

func TestRunErrors(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		measure      measureFunc
		wantExitCode int
		wantError    string
	}{
		{name: "invalid flag", args: []string{"--runs=many"}, wantExitCode: 2, wantError: "invalid value"},
		{name: "positional argument", args: []string{"extra"}, wantExitCode: 2, wantError: "unexpected positional arguments"},
		{name: "invalid batches", args: []string{"--batches=1,nope"}, wantExitCode: 1, wantError: "positive integer"},
		{
			name: "measurement error",
			measure: func(context.Context, clickhouseinfra.Config, performance.Options) (performance.Report, error) {
				return performance.Report{}, errors.New("measurement failed")
			},
			wantExitCode: 1,
			wantError:    "measurement failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measure := tt.measure
			if measure == nil {
				measure = func(context.Context, clickhouseinfra.Config, performance.Options) (performance.Report, error) {
					return performance.Report{}, nil
				}
			}
			var stderr bytes.Buffer
			exitCode := run(context.Background(), tt.args, bytes.NewBuffer(nil), &stderr, measure)
			if exitCode != tt.wantExitCode {
				t.Errorf("run() exit code = %d, want %d", exitCode, tt.wantExitCode)
			}
			if !strings.Contains(stderr.String(), tt.wantError) {
				t.Errorf("stderr = %q, want text containing %q", stderr.String(), tt.wantError)
			}
		})
	}
}

func TestRunHelp(t *testing.T) {
	var stderr bytes.Buffer
	if exitCode := run(context.Background(), []string{"--help"}, bytes.NewBuffer(nil), &stderr, nil); exitCode != 0 {
		t.Errorf("run() exit code = %d, want 0", exitCode)
	}
	if !strings.Contains(stderr.String(), "Usage of performance") {
		t.Errorf("help output = %q", stderr.String())
	}
}

func TestParseBatchSizes(t *testing.T) {
	if _, err := parseBatchSizes("1,,100"); err == nil {
		t.Fatal("parseBatchSizes() error = nil")
	}
	if _, err := parseBatchSizes("0"); err == nil {
		t.Fatal("parseBatchSizes() accepted zero")
	}
}
