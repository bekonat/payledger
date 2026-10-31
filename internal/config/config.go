package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Ledger struct {
	HTTPAddr        string
	GRPCAddr        string
	DatabaseURL     string
	RedisURL        string
	RateLimitRPS    float64
	RateLimitBurst  int
	OTLPEndpoint    string
	ShutdownTimeout time.Duration
}

type Relay struct {
	DatabaseURL  string
	KafkaBrokers []string
	MetricsAddr  string
	BatchSize    int
	PollInterval time.Duration
	MaxBackoff   time.Duration
	Retention    time.Duration
	OTLPEndpoint string
}

type Notifier struct {
	HTTPAddr        string
	DatabaseURL     string
	KafkaBrokers    []string
	GroupID         string
	MaxAttempts     int
	RetryBackoff    time.Duration
	OTLPEndpoint    string
	ShutdownTimeout time.Duration
}

func LoadLedger() (Ledger, error) {
	l := loader{}
	cfg := Ledger{
		HTTPAddr:        l.str("HTTP_ADDR", ":8080"),
		GRPCAddr:        l.str("GRPC_ADDR", ":9090"),
		DatabaseURL:     l.required("DATABASE_URL"),
		RedisURL:        l.str("REDIS_URL", ""),
		RateLimitRPS:    l.float("RATE_LIMIT_RPS", 50),
		RateLimitBurst:  l.int("RATE_LIMIT_BURST", 100),
		OTLPEndpoint:    l.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	if cfg.RateLimitRPS <= 0 {
		l.fail("RATE_LIMIT_RPS", "must be positive")
	}
	if cfg.RateLimitBurst < 1 {
		l.fail("RATE_LIMIT_BURST", "must be at least 1")
	}
	return cfg, l.err()
}

func LoadRelay() (Relay, error) {
	l := loader{}
	cfg := Relay{
		DatabaseURL:  l.required("DATABASE_URL"),
		KafkaBrokers: l.list("KAFKA_BROKERS", "localhost:9092"),
		MetricsAddr:  l.str("METRICS_ADDR", ":8082"),
		BatchSize:    l.int("OUTBOX_BATCH_SIZE", 100),
		PollInterval: l.duration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
		MaxBackoff:   l.duration("OUTBOX_MAX_BACKOFF", 10*time.Second),
		Retention:    l.duration("OUTBOX_RETENTION", 72*time.Hour),
		OTLPEndpoint: l.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
	if cfg.BatchSize < 1 {
		l.fail("OUTBOX_BATCH_SIZE", "must be at least 1")
	}
	return cfg, l.err()
}

func LoadNotifier() (Notifier, error) {
	l := loader{}
	cfg := Notifier{
		HTTPAddr:        l.str("HTTP_ADDR", ":8081"),
		DatabaseURL:     l.required("DATABASE_URL"),
		KafkaBrokers:    l.list("KAFKA_BROKERS", "localhost:9092"),
		GroupID:         l.str("KAFKA_GROUP_ID", "notifier"),
		MaxAttempts:     l.int("MAX_ATTEMPTS", 5),
		RetryBackoff:    l.duration("RETRY_BACKOFF", 200*time.Millisecond),
		OTLPEndpoint:    l.str("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	if cfg.MaxAttempts < 1 {
		l.fail("MAX_ATTEMPTS", "must be at least 1")
	}
	return cfg, l.err()
}

// loader collects every problem instead of stopping at the first one, so a
// misconfigured deployment reports all bad variables in a single log line.
type loader struct {
	errs []string
}

func (l *loader) fail(key, msg string) {
	l.errs = append(l.errs, key+" "+msg)
}

func (l *loader) err() error {
	if len(l.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %s", strings.Join(l.errs, "; "))
}

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (l *loader) required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		l.fail(key, "is required")
	}
	return v
}

func (l *loader) list(key, def string) []string {
	var out []string
	for _, p := range strings.Split(l.str(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		l.fail(key, "must not be empty")
	}
	return out
}

func (l *loader) int(key string, def int) int {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail(key, "must be an integer")
	}
	return n
}

func (l *loader) float(key string, def float64) float64 {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.fail(key, "must be a number")
	}
	return f
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(key, "must be a duration like 500ms or 10s")
	}
	return d
}
