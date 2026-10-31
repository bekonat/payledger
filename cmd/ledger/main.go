package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	ledgerv1 "github.com/bekonat/payledger/gen/ledger/v1"
	"github.com/bekonat/payledger/internal/config"
	"github.com/bekonat/payledger/internal/ledger"
	"github.com/bekonat/payledger/internal/observability"
	"github.com/bekonat/payledger/internal/postgres"
	"github.com/bekonat/payledger/internal/ratelimit"
	"github.com/bekonat/payledger/internal/transport/grpcapi"
	"github.com/bekonat/payledger/internal/transport/httpapi"
	"github.com/bekonat/payledger/migrations"
)

func main() {
	log := observability.NewLogger("ledger")
	if err := run(log); err != nil {
		log.Error("fatal", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.LoadLedger()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, "ledger", cfg.OTLPEndpoint)
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
	if err := postgres.Migrate(ctx, pool, migrations.Ledger, "ledger_schema_migrations"); err != nil {
		return err
	}

	// A nil *ratelimit.Limiter stored in the interface would not be == nil,
	// so the interface variable is assigned only when Redis is configured.
	var limiter httpapi.Limiter
	if cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return err
		}
		rdb := redis.NewClient(opts)
		defer rdb.Close()
		limiter = ratelimit.New(rdb, cfg.RateLimitRPS, cfg.RateLimitBurst)
		log.Info("rate limiting enabled", slog.Float64("rps", cfg.RateLimitRPS), slog.Int("burst", cfg.RateLimitBurst))
	} else {
		log.Warn("REDIS_URL is not set, rate limiting disabled")
	}

	store := postgres.NewStore(pool)
	svc := ledger.NewService(store)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/", httpapi.NewHandler(svc, store, limiter, log).Routes())
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(grpcapi.UnaryInterceptor(log)),
	)
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, grpcapi.NewServer(svc, log))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)
	reflection.Register(grpcSrv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http server started", slog.String("addr", cfg.HTTPAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		log.Info("grpc server started", slog.String("addr", cfg.GRPCAddr))
		return grpcSrv.Serve(lis)
	})
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down")
		// Report NOT_SERVING first so load balancers stop sending new calls.
		healthSrv.Shutdown()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()

		done := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(done)
		}()
		err := httpSrv.Shutdown(shutdownCtx)
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcSrv.Stop()
		}
		return err
	})
	return g.Wait()
}
