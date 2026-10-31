package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
)

// querier is the subset of methods shared by *pgxpool.Pool and pgx.Tx, so
// the same query helpers work inside and outside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) InTx(ctx context.Context, fn func(ctx context.Context, tx ledger.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(ctx, &Tx{q: tx})
	})
}

const accountColumns = `id, owner_id, currency, balance, is_system, created_at, updated_at`

func scanAccount(row pgx.Row) (*domain.Account, error) {
	var a domain.Account
	err := row.Scan(&a.ID, &a.OwnerID, &a.Currency, &a.Balance, &a.System, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// pgx returns timestamptz in the local zone; the API always speaks UTC.
	a.CreatedAt, a.UpdatedAt = a.CreatedAt.UTC(), a.UpdatedAt.UTC()
	return &a, nil
}

const transferColumns = `id, kind, from_account_id, to_account_id, amount, currency, created_at`

func scanTransfer(row pgx.Row) (*domain.Transfer, error) {
	var t domain.Transfer
	err := row.Scan(&t.ID, &t.Kind, &t.FromAccountID, &t.ToAccountID, &t.Amount, &t.Currency, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.CreatedAt = t.CreatedAt.UTC()
	return &t, nil
}

func getTransfer(ctx context.Context, q querier, id uuid.UUID) (*domain.Transfer, error) {
	return scanTransfer(q.QueryRow(ctx, `SELECT `+transferColumns+` FROM transfers WHERE id = $1`, id))
}

func (s *Store) CreateAccount(ctx context.Context, a *domain.Account) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO accounts (id, owner_id, currency, balance, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		a.ID, a.OwnerID, a.Currency, a.Balance, a.CreatedAt, a.UpdatedAt)
	return err
}

func (s *Store) GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return scanAccount(s.pool.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id))
}

func (s *Store) GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	return getTransfer(ctx, s.pool, id)
}

func (s *Store) ListEntries(ctx context.Context, accountID uuid.UUID, beforeID int64, limit int) ([]domain.Entry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, transfer_id, account_id, amount, balance_after, created_at
		FROM entries
		WHERE account_id = $1 AND ($2 = 0 OR id < $2)
		ORDER BY id DESC
		LIMIT $3`, accountID, beforeID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Entry, error) {
		var e domain.Entry
		err := row.Scan(&e.ID, &e.TransferID, &e.AccountID, &e.Amount, &e.BalanceAfter, &e.CreatedAt)
		e.CreatedAt = e.CreatedAt.UTC()
		return e, err
	})
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type Tx struct {
	q querier
}

func (t *Tx) ClaimIdempotencyKey(ctx context.Context, key string, hash []byte) (*ledger.IdempotencyRecord, error) {
	// ON CONFLICT DO NOTHING waits for a concurrent transaction that inserted
	// the same key. If it commits we get no row back and read its record; if
	// it rolls back our insert goes through.
	tag, err := t.q.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2)
		ON CONFLICT (key) DO NOTHING`, key, hash)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var rec ledger.IdempotencyRecord
	var transferID *uuid.UUID
	err = t.q.QueryRow(ctx, `SELECT request_hash, transfer_id FROM idempotency_keys WHERE key = $1`, key).
		Scan(&rec.RequestHash, &transferID)
	if err != nil {
		return nil, err
	}
	if transferID == nil {
		return nil, fmt.Errorf("idempotency key %q has no transfer", key)
	}
	rec.TransferID = *transferID
	return &rec, nil
}

func (t *Tx) BindIdempotencyKey(ctx context.Context, key string, transferID uuid.UUID) error {
	_, err := t.q.Exec(ctx, `UPDATE idempotency_keys SET transfer_id = $2 WHERE key = $1`, key, transferID)
	return err
}

func (t *Tx) GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	return scanAccount(t.q.QueryRow(ctx, `SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id))
}

func (t *Tx) SystemAccountID(ctx context.Context, currency string) (uuid.UUID, error) {
	var id uuid.UUID
	err := t.q.QueryRow(ctx, `SELECT id FROM accounts WHERE is_system AND currency = $1`, currency).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, domain.ErrNotFound
	}
	return id, err
}

func (t *Tx) LockAccounts(ctx context.Context, ids ...uuid.UUID) (map[uuid.UUID]*domain.Account, error) {
	rows, err := t.q.Query(ctx, `
		SELECT `+accountColumns+` FROM accounts
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[uuid.UUID]*domain.Account, len(ids))
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, err
		}
		out[a.ID] = a
	}
	return out, rows.Err()
}

func (t *Tx) UpdateBalance(ctx context.Context, id uuid.UUID, balance int64) error {
	_, err := t.q.Exec(ctx, `UPDATE accounts SET balance = $2, updated_at = now() WHERE id = $1`, id, balance)
	return err
}

func (t *Tx) InsertTransfer(ctx context.Context, tr *domain.Transfer) error {
	_, err := t.q.Exec(ctx, `
		INSERT INTO transfers (id, kind, from_account_id, to_account_id, amount, currency, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tr.ID, tr.Kind, tr.FromAccountID, tr.ToAccountID, tr.Amount, tr.Currency, tr.CreatedAt)
	return err
}

func (t *Tx) InsertEntries(ctx context.Context, entries []domain.Entry) error {
	for _, e := range entries {
		_, err := t.q.Exec(ctx, `
			INSERT INTO entries (transfer_id, account_id, amount, balance_after, created_at)
			VALUES ($1, $2, $3, $4, $5)`,
			e.TransferID, e.AccountID, e.Amount, e.BalanceAfter, e.CreatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Tx) GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	return getTransfer(ctx, t.q, id)
}

func (t *Tx) EnqueueOutbox(ctx context.Context, msg ledger.OutboxMessage) error {
	_, err := t.q.Exec(ctx, `
		INSERT INTO outbox (topic, message_key, payload, headers) VALUES ($1, $2, $3, $4)`,
		msg.Topic, msg.Key, msg.Payload, msg.Headers)
	return err
}
