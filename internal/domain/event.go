package domain

import (
	"time"

	"github.com/google/uuid"
)

const TopicTransfers = "ledger.transfers"

const EventTransferCompleted = "transfer.completed"

// TransferEvent is the payload published to Kafka through the outbox.
type TransferEvent struct {
	EventID    uuid.UUID    `json:"event_id"`
	Type       string       `json:"type"`
	OccurredAt time.Time    `json:"occurred_at"`
	TransferID uuid.UUID    `json:"transfer_id"`
	Kind       TransferKind `json:"kind"`
	From       EventParty   `json:"from"`
	To         EventParty   `json:"to"`
	Amount     int64        `json:"amount"`
	Currency   string       `json:"currency"`
}

type EventParty struct {
	AccountID    uuid.UUID `json:"account_id"`
	OwnerID      string    `json:"owner_id"`
	BalanceAfter int64     `json:"balance_after"`
}
