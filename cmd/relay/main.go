package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/bekonat/payledger/internal/config"
	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/kafka"
	"github.com/bekonat/payledger/internal/observability"
	"github.com/bekonat/payledger/internal/outbox"
	"github.com/bekonat/payledger/internal/postgres"
)

func main() {
	log := observability.NewLogger("relay")
	if err := run(log); err != nil {
		log.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadRelay()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "relay", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background()) //nolint:errcheck

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return err
	}

	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers[0], 3, domain.TopicTransfers, domain.TopicTransfers+".dlq"); err != nil {
		return err
	}
	writer := kafka.NewWriter(cfg.KafkaBrokers)
	defer writer.Close()

	relay := outbox.NewRelay(postgres.NewOutboxSource(pool), writer, outbox.Config{
		BatchSize:    cfg.BatchSize,
		PollInterval: cfg.PollInterval,
		MaxBackoff:   cfg.MaxBackoff,
		Retention:    cfg.Retention,
	}, log)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("outbox relay started", slog.Int("batch_size", cfg.BatchSize))
		return relay.Run(gctx)
	})
	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
