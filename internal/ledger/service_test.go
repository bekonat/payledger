package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/bekonat/payledger/internal/domain"
)

func setup(t *testing.T) (*Service, *fakeStore) {
	t.Helper()
	st := newFakeStore()
	return NewService(st), st
}

func mustAccount(t *testing.T, svc *Service, owner, currency string) *domain.Account {
	t.Helper()
	a, err := svc.CreateAccount(context.Background(), domain.CreateAccountInput{OwnerID: owner, Currency: currency})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func balance(t *testing.T, svc *Service, id uuid.UUID) int64 {
	t.Helper()
	a, err := svc.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return a.Balance
}

func TestDepositWithdrawTransfer(t *testing.T) {
	ctx := context.Background()
	svc, st := setup(t)
	alice := mustAccount(t, svc, "alice", "KZT")
	bob := mustAccount(t, svc, "bob", "KZT")

	if _, err := svc.Deposit(ctx, alice.ID, 10_000, "d1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Transfer(ctx, alice.ID, bob.ID, 3_000, "t1"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Withdraw(ctx, bob.ID, 1_000, "w1"); err != nil {
		t.Fatal(err)
	}

	if got := balance(t, svc, alice.ID); got != 7_000 {
		t.Errorf("alice balance = %d, want 7000", got)
	}
	if got := balance(t, svc, bob.ID); got != 2_000 {
		t.Errorf("bob balance = %d, want 2000", got)
	}

	// Double-entry invariant: the balances of all accounts of one currency,
	// system account included, always sum to zero.
	var sum int64
	for _, a := range st.data.accounts {
		if a.Currency == "KZT" {
			sum += a.Balance
		}
	}
	if sum != 0 {
		t.Errorf("sum of KZT balances = %d, want 0", sum)
	}
	if len(st.data.entries) != 6 {
		t.Errorf("entries = %d, want 6", len(st.data.entries))
	}
	if len(st.data.outbox) != 3 {
		t.Errorf("outbox messages = %d, want 3", len(st.data.outbox))
	}
}

func TestTransferRejections(t *testing.T) {
	ctx := context.Background()
	svc, st := setup(t)
	alice := mustAccount(t, svc, "alice", "KZT")
	bob := mustAccount(t, svc, "bob", "KZT")
	dollars := mustAccount(t, svc, "carol", "USD")
	if _, err := svc.Deposit(ctx, alice.ID, 500, "seed"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		call func() error
		want error
	}{
		{"insufficient funds", func() error {
			_, err := svc.Transfer(ctx, alice.ID, bob.ID, 501, "k1")
			return err
		}, domain.ErrInsufficientFunds},
		{"currency mismatch", func() error {
			_, err := svc.Transfer(ctx, alice.ID, dollars.ID, 100, "k2")
			return err
		}, domain.ErrCurrencyMismatch},
		{"unknown account", func() error {
			_, err := svc.Transfer(ctx, alice.ID, uuid.New(), 100, "k3")
			return err
		}, domain.ErrNotFound},
		{"withdraw more than balance", func() error {
			_, err := svc.Withdraw(ctx, bob.ID, 1, "k4")
			return err
		}, domain.ErrInsufficientFunds},
		{"transfer from system account", func() error {
			_, err := svc.Transfer(ctx, st.systemID("KZT"), bob.ID, 1, "k5")
			return err
		}, domain.ErrSystemAccount},
		{"deposit to system account", func() error {
			_, err := svc.Deposit(ctx, st.systemID("KZT"), 1, "k6")
			return err
		}, domain.ErrSystemAccount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}

	if got := balance(t, svc, alice.ID); got != 500 {
		t.Errorf("failed transfers changed alice balance to %d", got)
	}
	if len(st.data.keys) != 1 {
		t.Errorf("failed transfers must not keep idempotency keys, have %d", len(st.data.keys))
	}
}

func TestIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	svc, st := setup(t)
	alice := mustAccount(t, svc, "alice", "KZT")

	first, err := svc.Deposit(ctx, alice.ID, 1_000, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Deposit(ctx, alice.ID, 1_000, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed || !second.Replayed {
		t.Fatalf("replayed flags: first=%v second=%v", first.Replayed, second.Replayed)
	}
	if first.Transfer.ID != second.Transfer.ID {
		t.Fatal("replay returned a different transfer")
	}
	if got := balance(t, svc, alice.ID); got != 1_000 {
		t.Fatalf("balance = %d, money moved twice", got)
	}
	if len(st.data.outbox) != 1 {
		t.Fatalf("replay must not publish another event, outbox = %d", len(st.data.outbox))
	}

	_, err = svc.Deposit(ctx, alice.ID, 2_000, "same-key")
	if !errors.Is(err, domain.ErrIdempotencyMismatch) {
		t.Fatalf("reusing key with another amount: got %v", err)
	}
}

func TestOutboxEventCarriesTraceContext(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	svc, st := setup(t)
	alice := mustAccount(t, svc, "alice", "KZT")
	bob := mustAccount(t, svc, "bob", "KZT")
	ctx, span := tp.Tracer("test").Start(context.Background(), "request")
	if _, err := svc.Deposit(ctx, alice.ID, 900, "d"); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Transfer(ctx, alice.ID, bob.ID, 400, "t")
	if err != nil {
		t.Fatal(err)
	}
	span.End()

	msg := st.data.outbox[1]
	if msg.Topic != domain.TopicTransfers || msg.Key != res.Transfer.ID.String() {
		t.Fatalf("unexpected topic/key: %s %s", msg.Topic, msg.Key)
	}
	if msg.Headers["traceparent"] == "" {
		t.Fatal("traceparent header is missing")
	}
	var ev domain.TransferEvent
	if err := json.Unmarshal(msg.Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.From.OwnerID != "alice" || ev.From.BalanceAfter != 500 || ev.To.BalanceAfter != 400 || ev.Amount != 400 {
		t.Fatalf("unexpected event: %+v", ev)
	}
}

func TestListEntriesPagination(t *testing.T) {
	ctx := context.Background()
	svc, _ := setup(t)
	alice := mustAccount(t, svc, "alice", "KZT")
	for i := range 5 {
		if _, err := svc.Deposit(ctx, alice.ID, int64(i+1), uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}

	page, err := svc.ListEntries(ctx, alice.ID, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	var amounts []int64
	for page != nil {
		for _, e := range page.Entries {
			amounts = append(amounts, e.Amount)
		}
		if page.NextCursor == 0 {
			break
		}
		if page, err = svc.ListEntries(ctx, alice.ID, page.NextCursor, 2); err != nil {
			t.Fatal(err)
		}
	}
	want := []int64{5, 4, 3, 2, 1}
	if len(amounts) != len(want) {
		t.Fatalf("got %v, want %v", amounts, want)
	}
	for i := range want {
		if amounts[i] != want[i] {
			t.Fatalf("got %v, want %v", amounts, want)
		}
	}
}

func TestSystemAccountIsHidden(t *testing.T) {
	svc, st := setup(t)
	if _, err := svc.GetAccount(context.Background(), st.systemID("KZT")); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("system account must look like it does not exist, got %v", err)
	}
}
