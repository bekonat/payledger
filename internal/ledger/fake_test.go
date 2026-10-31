package ledger

import (
	"context"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/bekonat/payledger/internal/domain"
)

// fakeStore keeps everything in maps. InTx works on a copy and swaps it in
// only on success, which is enough to test rollback behaviour of the service.
type fakeStore struct {
	mu   sync.Mutex
	data *fakeData
}

type fakeData struct {
	accounts  map[uuid.UUID]domain.Account
	transfers map[uuid.UUID]domain.Transfer
	entries   []domain.Entry
	keys      map[string]IdempotencyRecord
	outbox    []OutboxMessage
}

func newFakeStore() *fakeStore {
	s := &fakeStore{data: &fakeData{
		accounts:  map[uuid.UUID]domain.Account{},
		transfers: map[uuid.UUID]domain.Transfer{},
		keys:      map[string]IdempotencyRecord{},
	}}
	for _, c := range []string{"KZT", "USD"} {
		id := uuid.New()
		s.data.accounts[id] = domain.Account{ID: id, OwnerID: domain.SystemOwner, Currency: c, System: true}
	}
	return s
}

func (d *fakeData) clone() *fakeData {
	c := &fakeData{
		accounts:  make(map[uuid.UUID]domain.Account, len(d.accounts)),
		transfers: make(map[uuid.UUID]domain.Transfer, len(d.transfers)),
		entries:   append([]domain.Entry(nil), d.entries...),
		keys:      make(map[string]IdempotencyRecord, len(d.keys)),
		outbox:    append([]OutboxMessage(nil), d.outbox...),
	}
	for k, v := range d.accounts {
		c.accounts[k] = v
	}
	for k, v := range d.transfers {
		c.transfers[k] = v
	}
	for k, v := range d.keys {
		c.keys[k] = v
	}
	return c
}

func (s *fakeStore) InTx(ctx context.Context, fn func(context.Context, Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	work := s.data.clone()
	if err := fn(ctx, &fakeTx{d: work}); err != nil {
		return err
	}
	s.data = work
	return nil
}

func (s *fakeStore) CreateAccount(_ context.Context, a *domain.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.accounts[a.ID] = *a
	return nil
}

func (s *fakeStore) GetAccount(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.data.accounts[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &a, nil
}

func (s *fakeStore) GetTransfer(_ context.Context, id uuid.UUID) (*domain.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.data.transfers[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &t, nil
}

func (s *fakeStore) ListEntries(_ context.Context, accountID uuid.UUID, beforeID int64, limit int) ([]domain.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Entry
	for i := len(s.data.entries) - 1; i >= 0 && len(out) < limit; i-- {
		e := s.data.entries[i]
		if e.AccountID == accountID && (beforeID == 0 || e.ID < beforeID) {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) systemID(currency string) uuid.UUID {
	for _, a := range s.data.accounts {
		if a.System && a.Currency == currency {
			return a.ID
		}
	}
	return uuid.Nil
}

type fakeTx struct {
	d *fakeData
}

func (t *fakeTx) ClaimIdempotencyKey(_ context.Context, key string, hash []byte) (*IdempotencyRecord, error) {
	if rec, ok := t.d.keys[key]; ok {
		return &rec, nil
	}
	t.d.keys[key] = IdempotencyRecord{RequestHash: hash}
	return nil, nil
}

func (t *fakeTx) BindIdempotencyKey(_ context.Context, key string, id uuid.UUID) error {
	rec := t.d.keys[key]
	rec.TransferID = id
	t.d.keys[key] = rec
	return nil
}

func (t *fakeTx) GetAccount(_ context.Context, id uuid.UUID) (*domain.Account, error) {
	a, ok := t.d.accounts[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &a, nil
}

func (t *fakeTx) SystemAccountID(_ context.Context, currency string) (uuid.UUID, error) {
	for _, a := range t.d.accounts {
		if a.System && a.Currency == currency {
			return a.ID, nil
		}
	}
	return uuid.Nil, domain.ErrNotFound
}

func (t *fakeTx) LockAccounts(_ context.Context, ids ...uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	sorted := append([]uuid.UUID(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })
	out := map[uuid.UUID]*domain.Account{}
	for _, id := range sorted {
		if a, ok := t.d.accounts[id]; ok {
			out[id] = &a
		}
	}
	return out, nil
}

func (t *fakeTx) UpdateBalance(_ context.Context, id uuid.UUID, balance int64) error {
	a := t.d.accounts[id]
	a.Balance = balance
	t.d.accounts[id] = a
	return nil
}

func (t *fakeTx) InsertTransfer(_ context.Context, tr *domain.Transfer) error {
	t.d.transfers[tr.ID] = *tr
	return nil
}

func (t *fakeTx) InsertEntries(_ context.Context, entries []domain.Entry) error {
	for _, e := range entries {
		e.ID = int64(len(t.d.entries) + 1)
		t.d.entries = append(t.d.entries, e)
	}
	return nil
}

func (t *fakeTx) GetTransfer(_ context.Context, id uuid.UUID) (*domain.Transfer, error) {
	tr, ok := t.d.transfers[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &tr, nil
}

func (t *fakeTx) EnqueueOutbox(_ context.Context, msg OutboxMessage) error {
	t.d.outbox = append(t.d.outbox, msg)
	return nil
}
