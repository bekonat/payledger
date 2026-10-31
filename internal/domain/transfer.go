package domain

import (
	"time"

	"github.com/google/uuid"
)

type TransferKind string

const (
	KindDeposit    TransferKind = "deposit"
	KindWithdrawal TransferKind = "withdrawal"
	KindTransfer   TransferKind = "transfer"
)

// MaxAmount keeps sums of many transfers far away from int64 overflow.
const MaxAmount int64 = 1_000_000_000_000

type Transfer struct {
	ID             uuid.UUID    `json:"id"`
	Kind           TransferKind `json:"kind"`
	FromAccountID  uuid.UUID    `json:"from_account_id"`
	ToAccountID    uuid.UUID    `json:"to_account_id"`
	Amount         int64        `json:"amount"`
	Currency       string       `json:"currency"`
	IdempotencyKey string       `json:"-"`
	CreatedAt      time.Time    `json:"created_at"`
}

// Entry is one side of a transfer. Every transfer produces exactly two
// entries whose amounts sum to zero.
type Entry struct {
	ID           int64     `json:"id"`
	TransferID   uuid.UUID `json:"transfer_id"`
	AccountID    uuid.UUID `json:"account_id"`
	Amount       int64     `json:"amount"`
	BalanceAfter int64     `json:"balance_after"`
	CreatedAt    time.Time `json:"created_at"`
}

type TransferInput struct {
	Kind           TransferKind
	FromAccountID  uuid.UUID
	ToAccountID    uuid.UUID
	Amount         int64
	IdempotencyKey string
}

func (in TransferInput) Validate() error {
	if in.IdempotencyKey == "" {
		return invalid("Idempotency-Key", "is required")
	}
	if len(in.IdempotencyKey) > 128 {
		return invalid("Idempotency-Key", "must be at most 128 characters")
	}
	if in.Amount <= 0 {
		return invalid("amount", "must be positive")
	}
	if in.Amount > MaxAmount {
		return invalid("amount", "is too large")
	}
	switch in.Kind {
	case KindDeposit:
		if in.ToAccountID == uuid.Nil {
			return invalid("account_id", "is required")
		}
	case KindWithdrawal:
		if in.FromAccountID == uuid.Nil {
			return invalid("account_id", "is required")
		}
	case KindTransfer:
		if in.FromAccountID == uuid.Nil {
			return invalid("from_account_id", "is required")
		}
		if in.ToAccountID == uuid.Nil {
			return invalid("to_account_id", "is required")
		}
		if in.FromAccountID == in.ToAccountID {
			return ErrSameAccount
		}
	default:
		return invalid("kind", "unknown transfer kind")
	}
	return nil
}

type EntryPage struct {
	Entries    []Entry `json:"entries"`
	NextCursor int64   `json:"next_cursor,omitempty"`
}
