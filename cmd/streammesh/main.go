// Command streammesh is the engine binary and its synthetic load generator.
//
//	streammesh run  -config examples/pipelines.yaml
//	streammesh gen  -url http://localhost:8081/ingest -rate 2000 -duration 15s
//	streammesh check -config examples/pipelines.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JumanaBaharul/streammesh/internal/config"
	"github.com/JumanaBaharul/streammesh/internal/enrich"
	"github.com/JumanaBaharul/streammesh/internal/metrics"
	"github.com/JumanaBaharul/streammesh/internal/pipeline"
	"github.com/JumanaBaharul/streammesh/internal/server"
)

// version is stamped into releases.
const version = "1.0.0"

func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "gen":
			os.Exit(runGenerator(args[1:]))
		case "check":
			os.Exit(runCheck(args[1:]))
		case "version", "-version", "--version":
			fmt.Printf("streammesh %s\n", version)
			return
		case "run":
			args = args[1:]
		}
	}
	os.Exit(runServer(args))
}

func runServer(args []string) int {
	flags := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := flags.String("config", "examples/pipelines.yaml", "path to the pipeline configuration")
	logLevel := flags.String("log-level", "info", "log level: debug, info, warn, error")
	showVersion := flags.Bool("version", false, "print the version and exit")
	_ = flags.Parse(args)

	if *showVersion {
		fmt.Printf("streammesh %s\n", version)
		return 0
	}

	logger := newLogger(*logLevel)
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("cannot load configuration", "error", err.Error())
		return 1
	}

	engine, err := pipeline.New(pipeline.Options{
		Config:   cfg,
		Logger:   logger,
		Registry: metrics.New(),
		Enricher: enrich.New(nil),
	})
	if err != nil {
		logger.Error("cannot start pipeline", "error", err.Error())
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := engine.Start(ctx); err != nil {
		logger.Error("cannot start pipeline", "error", err.Error())
		return 1
	}

	control := server.New(engine, cfg, logger)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- control.Start(ctx)
	}()

	logger.Info("streammesh running",
		"version", version,
		"sources", len(cfg.Sources),
		"sinks", len(cfg.Sinks),
		"rules", len(cfg.Rules))

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, append([]os.Signal{os.Interrupt, syscall.SIGTERM}, reloadSignals()...)...)

	for {
		select {
		case err := <-serverErr:
			if err != nil {
				logger.Error("control plane stopped", "error", err.Error())
			}
			return shutdown(control, cfg, engine, logger, *configPath)
		case sig := <-signals:
			if isReloadSignal(sig) {
				reloadRules(cfg, engine, logger, *configPath)
				continue
			}
			logger.Info("shutdown signal received", "signal", sig.String())
			return shutdown(control, cfg, engine, logger, *configPath)
		}
	}
}

func shutdown(control *server.Server, cfg *config.Config, engine *pipeline.Pipeline, logger *slog.Logger, configPath string) int {
	if err := control.Close(); err != nil {
		logger.Warn("control plane close failed", "error", err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Pipeline.ShutdownGrace.Or(10*time.Second)+5*time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		logger.Error("shutdown reported an error", "error", err.Error())
		return 1
	}
	return 0
}

// reloadRules re-reads only the rules block, so a bad edit is rejected without
// disturbing the running configuration.
func reloadRules(current *config.Config, engine *pipeline.Pipeline, logger *slog.Logger, path string) {
	fresh, err := config.Load(path)
	if err != nil {
		logger.Error("rule reload failed, keeping current rules", "error", err.Error())
		return
	}
	if err := engine.ReloadRules(fresh.Rules); err != nil {
		logger.Error("rule reload failed, keeping current rules", "error", err.Error())
		return
	}
	current.Rules = fresh.Rules
	logger.Info("rules reloaded from file", "path", path, "count", len(fresh.Rules))
}

func runCheck(args []string) int {
	flags := flag.NewFlagSet("check", flag.ExitOnError)
	configPath := flags.String("config", "examples/pipelines.yaml", "path to the pipeline configuration")
	_ = flags.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid: %v\n", err)
		return 1
	}
	fmt.Printf("configuration is valid: %d source(s), %d sink(s), %d rule(s)\n",
		len(cfg.Sources), len(cfg.Sinks), len(cfg.Rules))
	return 0
}

func newLogger(level string) *slog.Logger {
	var parsed slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		parsed = slog.LevelDebug
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		parsed = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parsed})
	return slog.New(handler)
}

// runGenerator produces a synthetic event stream so throughput can be measured
// on real hardware instead of estimated.
func runGenerator(args []string) int {
	flags := flag.NewFlagSet("gen", flag.ExitOnError)
	url := flags.String("url", "http://localhost:8081/ingest", "ingest endpoint to post to")
	rate := flags.Int("rate", 2000, "target events per second")
	duration := flags.Duration("duration", 10*time.Second, "how long to run")
	batchSize := flags.Int("batch", 200, "events per HTTP request")
	sources := flags.Int("sources", 8, "number of synthetic sources")
	seed := flags.Int64("seed", 1, "random seed")
	token := flags.String("token", "", "bearer token for the ingest endpoint")
	workers := flags.Int("workers", 4, "number of concurrent posting goroutines")
	_ = flags.Parse(args)

	if *rate <= 0 || *duration <= 0 || *batchSize <= 0 || *workers <= 0 {
		fmt.Fprintln(os.Stderr, "rate, duration, batch and workers must all be positive")
		return 1
	}

	logger := newLogger("warn")
	generatorWorkers := *workers
	generator := newSyntheticGenerator(*url, *rate, *batchSize, *sources, generatorWorkers, *seed, *token, logger)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	fmt.Printf("generating ~%d events/s for %s against %s\n", *rate, duration.String(), *url)

	var wg sync.WaitGroup
	started := time.Now()
	for worker := 0; worker < generatorWorkers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			generator.work(ctx)
		}()
	}
	wg.Wait()

	elapsed := time.Since(started)
	stats := generator.summary()
	achieved := float64(stats.accepted) / elapsed.Seconds()

	fmt.Printf("\n--- generator summary ---\n")
	fmt.Printf("elapsed:      %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("sent:         %d events in %d requests\n", stats.sent, stats.requests)
	fmt.Printf("accepted:     %d\n", stats.accepted)
	fmt.Printf("failed:       %d requests\n", stats.failed)
	fmt.Printf("achieved:     %.0f events/s (target %d)\n", achieved, *rate)
	if stats.failed > 0 {
		return 1
	}
	return 0
}
