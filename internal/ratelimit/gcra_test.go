package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newLimiter(t *testing.T, rate float64, burst int) (*Limiter, *time.Time) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	l := New(rdb, rate, burst)
	now := time.Unix(1_700_000_000, 0)
	l.now = func() time.Time { return now }
	return l, &now
}

func TestBurstThenLimit(t *testing.T) {
	ctx := context.Background()
	l, _ := newLimiter(t, 10, 5)

	for i := range 5 {
		ok, _, err := l.Allow(ctx, "client")
		if err != nil || !ok {
			t.Fatalf("request %d: ok=%v err=%v, burst must be allowed", i, ok, err)
		}
	}
	ok, retry, err := l.Allow(ctx, "client")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("request over the burst must be limited")
	}
	if retry != 100*time.Millisecond {
		t.Fatalf("retry after = %v, want 100ms (1/rate)", retry)
	}
}

func TestTokensRefillOverTime(t *testing.T) {
	ctx := context.Background()
	l, now := newLimiter(t, 10, 1)

	if ok, _, _ := l.Allow(ctx, "c"); !ok {
		t.Fatal("first request must pass")
	}
	if ok, _, _ := l.Allow(ctx, "c"); ok {
		t.Fatal("second immediate request must be limited")
	}
	*now = now.Add(100 * time.Millisecond)
	if ok, _, _ := l.Allow(ctx, "c"); !ok {
		t.Fatal("request after one emission interval must pass")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	ctx := context.Background()
	l, _ := newLimiter(t, 1, 1)
	if ok, _, _ := l.Allow(ctx, "a"); !ok {
		t.Fatal("a must pass")
	}
	if ok, _, _ := l.Allow(ctx, "b"); !ok {
		t.Fatal("b must not be affected by a")
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	l := New(rdb, 1, 1)
	mr.Close()
	if _, _, err := l.Allow(context.Background(), "x"); err == nil {
		t.Fatal("expected an error when redis is unavailable")
	}
}
