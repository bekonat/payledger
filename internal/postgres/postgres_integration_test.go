//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
	"github.com/bekonat/payledger/internal/notifier"
	"github.com/bekonat/payledger/internal/outbox"
	"github.com/bekonat/payledger/internal/postgres"
	"github.com/bekonat/payledger/migrations"
)

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;
		DROP SCHEMA IF EXISTS notifier CASCADE;`)
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, migrations.Ledger, "ledger_schema_migrations"); err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, pool, migrations.Notifier, "notifier_schema_migrations"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func newAccounts(t *testing.T, svc *ledger.Service, n int, deposit int64) []uuid.UUID {
	t.Helper()
	ctx := context.Background()
	ids := make([]uuid.UUID, n)
	for i := range ids {
		a, err := svc.CreateAccount(ctx, domain.CreateAccountInput{OwnerID: fmt.Sprintf("user-%d", i), Currency: "KZT"})
		if err != nil {
			t.Fatal(err)
		}
		if deposit > 0 {
			if _, err := svc.Deposit(ctx, a.ID, deposit, "seed-"+a.ID.String()); err != nil {
				t.Fatal(err)
			}
		}
		ids[i] = a.ID
	}
	return ids
}

func TestMigrationsAreIdempotent(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- postgres.Migrate(ctx, pool, migrations.Ledger, "ledger_schema_migrations")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var systems int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE is_system`).Scan(&systems); err != nil {
		t.Fatal(err)
	}
	if systems != 4 {
		t.Fatalf("system accounts = %d, migrations ran more than once", systems)
	}
}

// TestConcurrentTransfersKeepInvariants fires random transfers between a few
// accounts from many goroutines. Afterwards no balance may be negative, the
// total must be unchanged and every balance must equal the sum of its entries.
func TestConcurrentTransfersKeepInvariants(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 5, 10_000)

	const workers, perWorker = 20, 50
	var ok, insufficient atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 42))
			for i := range perWorker {
				from, to := ids[r.IntN(len(ids))], ids[r.IntN(len(ids))]
				if from == to {
					continue
				}
				_, err := svc.Transfer(ctx, from, to, int64(r.IntN(3_000)+1), fmt.Sprintf("w%d-%d", w, i))
				switch {
				case err == nil:
					ok.Add(1)
				case errors.Is(err, domain.ErrInsufficientFunds):
					insufficient.Add(1)
				default:
					t.Errorf("transfer failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	t.Logf("transfers: %d ok, %d rejected for insufficient funds", ok.Load(), insufficient.Load())

	var total, negative, mismatched int64
	err := pool.QueryRow(ctx, `
		SELECT
			COALESCE(sum(balance) FILTER (WHERE NOT is_system), 0),
			count(*) FILTER (WHERE balance < 0 AND NOT is_system),
			(SELECT count(*) FROM accounts a
			 WHERE a.balance <> COALESCE((SELECT sum(amount) FROM entries e WHERE e.account_id = a.id), 0))
		FROM accounts WHERE currency = 'KZT'`).Scan(&total, &negative, &mismatched)
	if err != nil {
		t.Fatal(err)
	}
	if total != 50_000 {
		t.Errorf("total user balance = %d, want 50000", total)
	}
	if negative != 0 {
		t.Errorf("%d accounts went negative", negative)
	}
	if mismatched != 0 {
		t.Errorf("%d accounts disagree with their entries", mismatched)
	}

	var ledgerSum int64
	if err := pool.QueryRow(ctx, `SELECT sum(balance) FROM accounts WHERE currency = 'KZT'`).Scan(&ledgerSum); err != nil {
		t.Fatal(err)
	}
	if ledgerSum != 0 {
		t.Errorf("double-entry invariant broken: sum of all KZT balances = %d", ledgerSum)
	}
}

// TestOppositeTransfersDoNotDeadlock runs A->B and B->A at the same time.
// Without ordered locking this deadlocks quickly and Postgres aborts one side.
func TestOppositeTransfersDoNotDeadlock(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 2, 1_000_000)

	var wg sync.WaitGroup
	for dir := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			from, to := ids[dir], ids[1-dir]
			for i := range 200 {
				if _, err := svc.Transfer(ctx, from, to, 1, fmt.Sprintf("d%d-%d", dir, i)); err != nil {
					t.Errorf("transfer: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentDuplicateRequestsMoveMoneyOnce(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 2, 10_000)

	const clients = 10
	results := make(chan *ledger.TransferResult, clients)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.Transfer(ctx, ids[0], ids[1], 700, "retry-storm")
			if err != nil {
				t.Errorf("duplicate request failed: %v", err)
				return
			}
			results <- res
		}()
	}
	wg.Wait()
	close(results)

	var fresh int
	var transferID uuid.UUID
	for res := range results {
		if !res.Replayed {
			fresh++
		}
		if transferID == uuid.Nil {
			transferID = res.Transfer.ID
		} else if res.Transfer.ID != transferID {
			t.Fatal("duplicates returned different transfers")
		}
	}
	if fresh != 1 {
		t.Fatalf("%d requests executed the transfer, want exactly 1", fresh)
	}
	a, err := svc.GetAccount(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != 9_300 {
		t.Fatalf("balance = %d, want 9300", a.Balance)
	}
}

func TestFailedTransferRollsBackEverything(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 2, 100)

	var outboxBefore int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outboxBefore)

	if _, err := svc.Transfer(ctx, ids[0], ids[1], 101, "too-much"); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
	var keys, outboxAfter int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE key = 'too-much'`).Scan(&keys)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&outboxAfter)
	if keys != 0 || outboxAfter != outboxBefore {
		t.Fatalf("rollback left data behind: keys=%d outbox %d->%d", keys, outboxBefore, outboxAfter)
	}
	// The same key can be used again once the first attempt has failed.
	if _, err := svc.Transfer(ctx, ids[0], ids[1], 100, "too-much"); err != nil {
		t.Fatal(err)
	}
}

func TestEntriesPagination(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 1, 0)
	for i := range 7 {
		if _, err := svc.Deposit(ctx, ids[0], int64(i+1), fmt.Sprint("p", i)); err != nil {
			t.Fatal(err)
		}
	}
	var got []int64
	var cursor int64
	for {
		page, err := svc.ListEntries(ctx, ids[0], cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			got = append(got, e.Amount)
		}
		if page.NextCursor == 0 {
			break
		}
		cursor = page.NextCursor
	}
	if fmt.Sprint(got) != "[7 6 5 4 3 2 1]" {
		t.Fatalf("pages = %v", got)
	}
}

// TestOutboxSkipLocked claims batches from two relays at once. SKIP LOCKED
// must hand them disjoint rows, and every row must be published exactly once.
func TestOutboxSkipLocked(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	ids := newAccounts(t, svc, 1, 0)
	for i := range 40 {
		if _, err := svc.Deposit(ctx, ids[0], 1, fmt.Sprint("o", i)); err != nil {
			t.Fatal(err)
		}
	}
	src := postgres.NewOutboxSource(pool)

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := src.Claim(ctx, 5, func(_ context.Context, msgs []outbox.Message) error {
					time.Sleep(10 * time.Millisecond) // hold the locks so the batches overlap in time
					mu.Lock()
					defer mu.Unlock()
					for _, m := range msgs {
						seen[m.ID]++
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(seen) != 40 {
		t.Fatalf("published %d distinct messages, want 40", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %d published %d times", id, n)
		}
	}
	lag, err := src.OldestUnpublished(ctx)
	if err != nil || lag != 0 {
		t.Fatalf("lag = %v, err = %v; want 0 after draining", lag, err)
	}
}

func TestOutboxFailedPublishKeepsMessages(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	svc := ledger.NewService(postgres.NewStore(pool))
	newAccounts(t, svc, 1, 500)
	src := postgres.NewOutboxSource(pool)

	_, err := src.Claim(ctx, 10, func(context.Context, []outbox.Message) error { return errors.New("kafka down") })
	if err == nil {
		t.Fatal("expected error")
	}
	n, err := src.Claim(ctx, 10, func(context.Context, []outbox.Message) error { return nil })
	if err != nil || n != 1 {
		t.Fatalf("message must still be pending after a failed publish: n=%d err=%v", n, err)
	}

	_, _ = pool.Exec(ctx, `UPDATE outbox SET published_at = now() - interval '2 hours'`)
	deleted, err := src.DeletePublished(ctx, time.Hour)
	if err != nil || deleted != 1 {
		t.Fatalf("cleanup deleted %d, err %v", deleted, err)
	}
}

func TestNotificationStoreProcessOnce(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	store := postgres.NewNotificationStore(pool)
	eventID := uuid.NewString()
	ns := []notifier.Notification{{EventID: eventID, OwnerID: "alice", AccountID: uuid.NewString(), Message: "hi"}}

	for i, want := range []bool{true, false, false} {
		got, err := store.ProcessOnce(ctx, eventID, ns)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("delivery %d: processed=%v, want %v", i+1, got, want)
		}
	}
	list, err := store.List(ctx, "alice", 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("notifications = %d, err = %v; want exactly 1", len(list), err)
	}
}
