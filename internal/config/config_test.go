package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadLedgerDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/x")
	cfg, err := LoadLedger()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.GRPCAddr != ":9090" || cfg.RateLimitBurst != 100 || cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("RATE_LIMIT_RPS", "fast")
	t.Setenv("SHUTDOWN_TIMEOUT", "10")
	_, err := LoadLedger()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{"DATABASE_URL", "RATE_LIMIT_RPS", "SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}

func TestLoadRelayBrokers(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/x")
	t.Setenv("KAFKA_BROKERS", " k1:9092, ,k2:9092 ")
	cfg, err := LoadRelay()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "k2:9092" {
		t.Fatalf("brokers = %v", cfg.KafkaBrokers)
	}
}
