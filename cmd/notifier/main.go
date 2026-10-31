package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/bekonat/payledger/internal/config"
	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/kafka"
	"github.com/bekonat/payledger/internal/notifier"
	"github.com/bekonat/payledger/internal/observability"
	"github.com/bekonat/payledger/internal/postgres"
	"github.com/bekonat/payledger/migrations"
)

func main() {
	log := observability.NewLogger("notifier")
	if err := run(log); err != nil {
		log.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadNotifier()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "notifier", cfg.OTLPEndpoint)
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
	if err := postgres.Migrate(ctx, pool, migrations.Notifier, "notifier_schema_migrations"); err != nil {
		return err
	}

	dlqTopic := domain.TopicTransfers + ".dlq"
	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers[0], 3, domain.TopicTransfers, dlqTopic); err != nil {
		return err
	}
	reader := kafka.NewReader(cfg.KafkaBrokers, domain.TopicTransfers, cfg.GroupID)
	defer reader.Close()
	dlq := kafka.NewWriter(cfg.KafkaBrokers)
	defer dlq.Close()

	store := postgres.NewNotificationStore(pool)
	consumer := notifier.NewConsumer(reader, dlq, store, notifier.Config{
		MaxAttempts: cfg.MaxAttempts,
		Backoff:     cfg.RetryBackoff,
		DLQTopic:    dlqTopic,
	}, log)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /api/v1/notifications", func(w http.ResponseWriter, r *http.Request) {
		owner := r.URL.Query().Get("owner_id")
		if owner == "" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "owner_id is required"})
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		ns, err := store.List(r.Context(), owner, limit)
		if err != nil {
			log.ErrorContext(r.Context(), "list notifications", slog.String("error", err.Error()))
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"notifications": ns})
	})
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("consumer started", slog.String("group", cfg.GroupID))
		return consumer.Run(gctx)
	})
	g.Go(func() error {
		log.Info("http server started", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	return g.Wait()
}
