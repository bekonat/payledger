package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/observability"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

var tracer = otel.Tracer("github.com/bekonat/payledger/internal/ledger")

type Service struct {
	store Store
	now   func() time.Time
}

func NewService(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

type TransferResult struct {
	Transfer *domain.Transfer
	// Replayed is true when the idempotency key was seen before and the
	// stored transfer is returned without moving money again.
	Replayed bool
}

func (s *Service) CreateAccount(ctx context.Context, in domain.CreateAccountInput) (*domain.Account, error) {
	if err := in.Normalize(); err != nil {
		return nil, err
	}
	now := s.now().UTC()
	a := &domain.Account{
		ID:        uuid.Must(uuid.NewV7()),
		OwnerID:   in.OwnerID,
		Currency:  in.Currency,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.store.CreateAccount(ctx, a); err != nil {
		return nil, err
	}
	return a, nil
}

func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error) {
	a, err := s.store.GetAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	if a.System {
		return nil, domain.ErrNotFound
	}
	return a, nil
}

func (s *Service) GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error) {
	return s.store.GetTransfer(ctx, id)
}

// ListEntries returns the account statement newest first. Pagination is
// keyset-based: the cursor is the id of the last entry on the previous page,
// so deep pages cost the same as the first one, unlike OFFSET.
func (s *Service) ListEntries(ctx context.Context, accountID uuid.UUID, cursor int64, limit int) (*domain.EntryPage, error) {
	if limit <= 0 {
		limit = defaultPageSize
	}
	limit = min(limit, maxPageSize)
	if _, err := s.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	entries, err := s.store.ListEntries(ctx, accountID, cursor, limit)
	if err != nil {
		return nil, err
	}
	page := &domain.EntryPage{Entries: entries}
	if len(entries) == limit {
		page.NextCursor = entries[len(entries)-1].ID
	}
	return page, nil
}

func (s *Service) Deposit(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*TransferResult, error) {
	return s.execute(ctx, domain.TransferInput{
		Kind: domain.KindDeposit, ToAccountID: accountID, Amount: amount, IdempotencyKey: key,
	})
}

func (s *Service) Withdraw(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*TransferResult, error) {
	return s.execute(ctx, domain.TransferInput{
		Kind: domain.KindWithdrawal, FromAccountID: accountID, Amount: amount, IdempotencyKey: key,
	})
}

func (s *Service) Transfer(ctx context.Context, from, to uuid.UUID, amount int64, key string) (*TransferResult, error) {
	return s.execute(ctx, domain.TransferInput{
		Kind: domain.KindTransfer, FromAccountID: from, ToAccountID: to, Amount: amount, IdempotencyKey: key,
	})
}

func (s *Service) execute(ctx context.Context, in domain.TransferInput) (res *TransferResult, err error) {
	ctx, span := tracer.Start(ctx, "ledger."+string(in.Kind), trace.WithAttributes(
		attribute.String("transfer.kind", string(in.Kind)),
		attribute.Int64("transfer.amount", in.Amount),
	))
	defer func() {
		observability.TransfersTotal.WithLabelValues(string(in.Kind), resultLabel(res, err)).Inc()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if err := in.Validate(); err != nil {
		return nil, err
	}

	err = s.store.InTx(ctx, func(ctx context.Context, tx Tx) error {
		res, err = s.executeTx(ctx, tx, in)
		return err
	})
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.String("transfer.id", res.Transfer.ID.String()), attribute.Bool("transfer.replayed", res.Replayed))
	return res, nil
}

func (s *Service) executeTx(ctx context.Context, tx Tx, in domain.TransferInput) (*TransferResult, error) {
	hash := requestHash(in)
	rec, err := tx.ClaimIdempotencyKey(ctx, in.IdempotencyKey, hash)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		if !bytes.Equal(rec.RequestHash, hash) {
			return nil, domain.ErrIdempotencyMismatch
		}
		t, err := tx.GetTransfer(ctx, rec.TransferID)
		if err != nil {
			return nil, fmt.Errorf("load transfer for idempotency key: %w", err)
		}
		return &TransferResult{Transfer: t, Replayed: true}, nil
	}

	from, to, err := s.resolveParties(ctx, tx, in)
	if err != nil {
		return nil, err
	}

	accounts, err := tx.LockAccounts(ctx, from, to)
	if err != nil {
		return nil, err
	}
	src, dst := accounts[from], accounts[to]
	if src == nil || dst == nil {
		return nil, domain.ErrNotFound
	}
	if in.Kind == domain.KindTransfer && (src.System || dst.System) {
		return nil, domain.ErrSystemAccount
	}
	if src.Currency != dst.Currency {
		return nil, domain.ErrCurrencyMismatch
	}
	if !src.System && src.Balance < in.Amount {
		return nil, domain.ErrInsufficientFunds
	}

	src.Balance -= in.Amount
	dst.Balance += in.Amount
	now := s.now().UTC()

	t := &domain.Transfer{
		ID:             uuid.Must(uuid.NewV7()),
		Kind:           in.Kind,
		FromAccountID:  src.ID,
		ToAccountID:    dst.ID,
		Amount:         in.Amount,
		Currency:       src.Currency,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      now,
	}
	if err := tx.InsertTransfer(ctx, t); err != nil {
		return nil, err
	}
	if err := tx.UpdateBalance(ctx, src.ID, src.Balance); err != nil {
		return nil, err
	}
	if err := tx.UpdateBalance(ctx, dst.ID, dst.Balance); err != nil {
		return nil, err
	}
	if err := tx.InsertEntries(ctx, []domain.Entry{
		{TransferID: t.ID, AccountID: src.ID, Amount: -in.Amount, BalanceAfter: src.Balance, CreatedAt: now},
		{TransferID: t.ID, AccountID: dst.ID, Amount: in.Amount, BalanceAfter: dst.Balance, CreatedAt: now},
	}); err != nil {
		return nil, err
	}
	if err := tx.BindIdempotencyKey(ctx, in.IdempotencyKey, t.ID); err != nil {
		return nil, err
	}
	msg, err := transferMessage(ctx, t, src, dst, now)
	if err != nil {
		return nil, err
	}
	if err := tx.EnqueueOutbox(ctx, msg); err != nil {
		return nil, err
	}
	return &TransferResult{Transfer: t}, nil
}

// resolveParties replaces the missing side of a deposit or withdrawal with
// the system account of the same currency. Currency never changes after an
// account is created, so reading it without a lock is safe. The read goes
// through tx, not the pool: asking the pool for a second connection while
// holding one can exhaust it under load and hang every transaction.
func (s *Service) resolveParties(ctx context.Context, tx Tx, in domain.TransferInput) (from, to uuid.UUID, err error) {
	if in.Kind == domain.KindTransfer {
		return in.FromAccountID, in.ToAccountID, nil
	}
	userID := in.ToAccountID
	if in.Kind == domain.KindWithdrawal {
		userID = in.FromAccountID
	}
	acc, err := tx.GetAccount(ctx, userID)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if acc.System {
		return uuid.Nil, uuid.Nil, domain.ErrSystemAccount
	}
	sys, err := tx.SystemAccountID(ctx, acc.Currency)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	if in.Kind == domain.KindDeposit {
		return sys, userID, nil
	}
	return userID, sys, nil
}

func transferMessage(ctx context.Context, t *domain.Transfer, src, dst *domain.Account, now time.Time) (OutboxMessage, error) {
	ev := domain.TransferEvent{
		EventID:    uuid.Must(uuid.NewV7()),
		Type:       domain.EventTransferCompleted,
		OccurredAt: now,
		TransferID: t.ID,
		Kind:       t.Kind,
		From:       domain.EventParty{AccountID: src.ID, OwnerID: src.OwnerID, BalanceAfter: src.Balance},
		To:         domain.EventParty{AccountID: dst.ID, OwnerID: dst.OwnerID, BalanceAfter: dst.Balance},
		Amount:     t.Amount,
		Currency:   t.Currency,
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return OutboxMessage{}, err
	}
	// The trace context travels with the event: relay -> Kafka -> notifier
	// all show up in the same trace as the HTTP request that caused them.
	headers := map[string]string{"event_type": ev.Type}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return OutboxMessage{
		Topic:   domain.TopicTransfers,
		Key:     t.ID.String(),
		Payload: payload,
		Headers: headers,
	}, nil
}

func requestHash(in domain.TransferInput) []byte {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%d", in.Kind, in.FromAccountID, in.ToAccountID, in.Amount)
	return h.Sum(nil)
}

func resultLabel(res *TransferResult, err error) string {
	var ve *domain.ValidationError
	switch {
	case err == nil && res.Replayed:
		return "replayed"
	case err == nil:
		return "ok"
	case errors.As(err, &ve):
		return "invalid"
	case errors.Is(err, domain.ErrInsufficientFunds):
		return "insufficient_funds"
	case errors.Is(err, domain.ErrNotFound):
		return "not_found"
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		return "idempotency_mismatch"
	case errors.Is(err, domain.ErrCurrencyMismatch), errors.Is(err, domain.ErrSameAccount), errors.Is(err, domain.ErrSystemAccount):
		return "rejected"
	default:
		return "error"
	}
}
