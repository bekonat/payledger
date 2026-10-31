package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/kafka"
)

type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
	drained   chan struct{}
}

func newFakeReader(msgs ...kafka.Message) *fakeReader {
	return &fakeReader{msgs: msgs, drained: make(chan struct{})}
}

func (r *fakeReader) Fetch(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) Commit(_ context.Context, m kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.committed = append(r.committed, m.Offset)
	if len(r.msgs) == 0 {
		select {
		case <-r.drained:
		default:
			close(r.drained)
		}
	}
	return nil
}

type fakeStore struct {
	mu        sync.Mutex
	seen      map[string]bool
	saved     []Notification
	failTimes int
}

func (s *fakeStore) ProcessOnce(_ context.Context, eventID string, ns []Notification) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failTimes > 0 {
		s.failTimes--
		return false, errors.New("database is down")
	}
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	if s.seen[eventID] {
		return false, nil
	}
	s.seen[eventID] = true
	s.saved = append(s.saved, ns...)
	return true, nil
}

func (s *fakeStore) List(context.Context, string, int) ([]Notification, error) { return nil, nil }

type fakeDLQ struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (d *fakeDLQ) Publish(_ context.Context, msgs []kafka.Message) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.msgs = append(d.msgs, msgs...)
	return nil
}

func event(t *testing.T, id uuid.UUID, offset int64) kafka.Message {
	t.Helper()
	b, err := json.Marshal(domain.TransferEvent{
		EventID: id, Type: domain.EventTransferCompleted, Kind: domain.KindTransfer,
		From:   domain.EventParty{AccountID: uuid.New(), OwnerID: "alice", BalanceAfter: 5_000},
		To:     domain.EventParty{AccountID: uuid.New(), OwnerID: "bob", BalanceAfter: 2_550},
		Amount: 2_550, Currency: "KZT",
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: domain.TopicTransfers, Value: b, Offset: offset, Headers: map[string]string{}}
}

func run(t *testing.T, r *fakeReader, store Store, dlq *fakeDLQ, attempts int) {
	t.Helper()
	c := NewConsumer(r, dlq, store, Config{MaxAttempts: attempts, Backoff: time.Millisecond, DLQTopic: "dlq"},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	select {
	case <-r.drained:
	case <-time.After(3 * time.Second):
		t.Fatal("messages were not processed in time")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDuplicateDeliveryIsProcessedOnce(t *testing.T) {
	id := uuid.New()
	r := newFakeReader(event(t, id, 1), event(t, id, 2))
	store := &fakeStore{}
	run(t, r, store, &fakeDLQ{}, 3)

	if len(store.saved) != 2 {
		t.Fatalf("saved %d notifications, want 2 (one per party, once)", len(store.saved))
	}
	if len(r.committed) != 2 {
		t.Fatalf("both offsets must be committed, got %v", r.committed)
	}
}

func TestTransientErrorIsRetried(t *testing.T) {
	r := newFakeReader(event(t, uuid.New(), 7))
	store := &fakeStore{failTimes: 2}
	dlq := &fakeDLQ{}
	run(t, r, store, dlq, 5)

	if len(store.saved) != 2 || len(dlq.msgs) != 0 {
		t.Fatalf("saved=%d dlq=%d, want 2 saved and empty DLQ", len(store.saved), len(dlq.msgs))
	}
}

func TestPoisonMessageGoesToDLQ(t *testing.T) {
	bad := kafka.Message{Topic: domain.TopicTransfers, Value: []byte("not json"), Offset: 3, Headers: map[string]string{}}
	r := newFakeReader(bad, event(t, uuid.New(), 4))
	store := &fakeStore{}
	dlq := &fakeDLQ{}
	run(t, r, store, dlq, 5)

	if len(dlq.msgs) != 1 || dlq.msgs[0].Topic != "dlq" || dlq.msgs[0].Headers["dlq_source_offset"] != "3" {
		t.Fatalf("unexpected DLQ content: %+v", dlq.msgs)
	}
	if len(store.saved) != 2 {
		t.Fatal("the message after the poison one must still be processed")
	}
	if len(r.committed) != 2 {
		t.Fatalf("poison message offset must be committed too, got %v", r.committed)
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	r := newFakeReader(event(t, uuid.New(), 9))
	store := &fakeStore{failTimes: 100}
	dlq := &fakeDLQ{}
	run(t, r, store, dlq, 3)
	if len(dlq.msgs) != 1 {
		t.Fatalf("dlq = %d, want 1", len(dlq.msgs))
	}
}

func TestBuildNotifications(t *testing.T) {
	sys := domain.EventParty{AccountID: uuid.New(), OwnerID: domain.SystemOwner, BalanceAfter: -100_00}
	user := domain.EventParty{AccountID: uuid.New(), OwnerID: "alice", BalanceAfter: 100_00}

	ns := BuildNotifications(domain.TransferEvent{EventID: uuid.New(), Kind: domain.KindDeposit, From: sys, To: user, Amount: 100_00, Currency: "KZT"})
	if len(ns) != 1 || ns[0].OwnerID != "alice" {
		t.Fatalf("deposit must notify only the user: %+v", ns)
	}
	if want := "Deposit +100.00 KZT. Balance: 100.00 KZT"; ns[0].Message != want {
		t.Fatalf("message = %q, want %q", ns[0].Message, want)
	}
}

func TestFormatAmount(t *testing.T) {
	tests := map[int64]string{0: "0.00 KZT", 5: "0.05 KZT", 123456: "1234.56 KZT", -250: "-2.50 KZT"}
	for in, want := range tests {
		if got := FormatAmount(in, "KZT"); got != want {
			t.Errorf("FormatAmount(%d) = %q, want %q", in, got, want)
		}
	}
}
