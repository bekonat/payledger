package ledger

import (
	"context"

	"github.com/google/uuid"

	"github.com/bekonat/payledger/internal/domain"
)

// Store is implemented by the postgres package. Reads that do not need
// locks live on Store, everything that changes money goes through Tx.
type Store interface {
	InTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	CreateAccount(ctx context.Context, a *domain.Account) error
	GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
	ListEntries(ctx context.Context, accountID uuid.UUID, beforeID int64, limit int) ([]domain.Entry, error)
}

type Tx interface {
	// ClaimIdempotencyKey inserts the key. If the key already exists it
	// returns the stored record instead. A concurrent transaction holding the
	// same key makes this call wait until that transaction finishes.
	ClaimIdempotencyKey(ctx context.Context, key string, requestHash []byte) (*IdempotencyRecord, error)
	BindIdempotencyKey(ctx context.Context, key string, transferID uuid.UUID) error
	GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	SystemAccountID(ctx context.Context, currency string) (uuid.UUID, error)
	// LockAccounts takes row locks in ascending id order, so two transfers
	// between the same accounts in opposite directions cannot deadlock.
	LockAccounts(ctx context.Context, ids ...uuid.UUID) (map[uuid.UUID]*domain.Account, error)
	UpdateBalance(ctx context.Context, id uuid.UUID, balance int64) error
	InsertTransfer(ctx context.Context, t *domain.Transfer) error
	InsertEntries(ctx context.Context, entries []domain.Entry) error
	GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
	EnqueueOutbox(ctx context.Context, msg OutboxMessage) error
}

type IdempotencyRecord struct {
	RequestHash []byte
	TransferID  uuid.UUID
}

type OutboxMessage struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}
