package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bekonat/payledger/internal/kafka"
)

type fakeSource struct {
	mu        sync.Mutex
	pending   []Message
	published []Message
	claims    int
}

func (s *fakeSource) Claim(ctx context.Context, limit int, fn func(context.Context, []Message) error) (int, error) {
	s.mu.Lock()
	s.claims++
	n := min(limit, len(s.pending))
	batch := append([]Message(nil), s.pending[:n]...)
	s.mu.Unlock()

	if err := fn(ctx, batch); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = s.pending[n:]
	s.published = append(s.published, batch...)
	return n, nil
}

func (s *fakeSource) OldestUnpublished(context.Context) (time.Duration, error) { return 0, nil }

func (s *fakeSource) DeletePublished(context.Context, time.Duration) (int64, error) { return 0, nil }

func (s *fakeSource) state() (pending, published, claims int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending), len(s.published), s.claims
}

type fakePublisher struct {
	mu       sync.Mutex
	failures int
	got      []kafka.Message
}

func (p *fakePublisher) Publish(_ context.Context, msgs []kafka.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("broker unavailable")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func messages(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{ID: int64(i + 1), Topic: "t", Key: "k", Payload: []byte("{}"), Headers: map[string]string{}}
	}
	return out
}

func runRelay(t *testing.T, src Source, pub Publisher, cfg Config, until func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	r := NewRelay(src, pub, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for !until() {
		select {
		case <-deadline:
			cancel()
			t.Fatal("condition not reached in time")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestRelayDrainsBacklogWithoutWaiting(t *testing.T) {
	src := &fakeSource{pending: messages(250)}
	pub := &fakePublisher{}
	// A long poll interval proves that full batches are followed immediately
	// by the next batch instead of sleeping between them.
	cfg := Config{BatchSize: 100, PollInterval: time.Hour, MaxBackoff: time.Hour}

	runRelay(t, src, pub, cfg, func() bool {
		_, published, _ := src.state()
		return published == 250
	})
	if len(pub.got) != 250 {
		t.Fatalf("published %d messages, want 250", len(pub.got))
	}
}

func TestRelayRetriesFailedBatch(t *testing.T) {
	src := &fakeSource{pending: messages(3)}
	pub := &fakePublisher{failures: 2}
	cfg := Config{BatchSize: 10, PollInterval: time.Millisecond, MaxBackoff: 4 * time.Millisecond}

	runRelay(t, src, pub, cfg, func() bool {
		_, published, _ := src.state()
		return published == 3
	})
	_, _, claims := src.state()
	if claims < 3 {
		t.Fatalf("expected at least 3 claims (2 failures + success), got %d", claims)
	}
	if len(pub.got) != 3 {
		t.Fatalf("published %d messages, want exactly 3", len(pub.got))
	}
}

func TestRelayStopsOnCancel(t *testing.T) {
	src := &fakeSource{}
	r := NewRelay(src, &fakePublisher{}, Config{BatchSize: 10, PollInterval: time.Hour, MaxBackoff: time.Hour},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after cancel")
	}
}
