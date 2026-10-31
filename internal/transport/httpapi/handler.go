package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/bekonat/payledger/api"
	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
)

type LedgerService interface {
	CreateAccount(ctx context.Context, in domain.CreateAccountInput) (*domain.Account, error)
	GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	ListEntries(ctx context.Context, accountID uuid.UUID, cursor int64, limit int) (*domain.EntryPage, error)
	GetTransfer(ctx context.Context, id uuid.UUID) (*domain.Transfer, error)
	Deposit(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
	Withdraw(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
	Transfer(ctx context.Context, from, to uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
}

type HealthChecker interface {
	Ping(ctx context.Context) error
}

type Handler struct {
	svc     LedgerService
	health  HealthChecker
	limiter Limiter
	log     *slog.Logger
}

func NewHandler(svc LedgerService, health HealthChecker, limiter Limiter, log *slog.Logger) *Handler {
	return &Handler{svc: svc, health: health, limiter: limiter, log: log}
}

func (h *Handler) Routes() http.Handler {
	limited := rateLimit(h.limiter, h.log)
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/accounts", limited(http.HandlerFunc(h.createAccount)))
	mux.Handle("GET /api/v1/accounts/{id}", limited(http.HandlerFunc(h.getAccount)))
	mux.Handle("GET /api/v1/accounts/{id}/entries", limited(http.HandlerFunc(h.listEntries)))
	mux.Handle("POST /api/v1/deposits", limited(http.HandlerFunc(h.deposit)))
	mux.Handle("POST /api/v1/withdrawals", limited(http.HandlerFunc(h.withdraw)))
	mux.Handle("POST /api/v1/transfers", limited(http.HandlerFunc(h.transfer)))
	mux.Handle("GET /api/v1/transfers/{id}", limited(http.HandlerFunc(h.getTransfer)))
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(api.OpenAPI)
	})

	// metrics must wrap the mux directly: the mux writes the matched pattern
	// into this exact *http.Request, and metrics reads it after the call.
	return chain(mux, tracing, requestID, logging(h.log), recoverer(h.log), metrics)
}

func (h *Handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.health.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) createAccount(w http.ResponseWriter, r *http.Request) {
	var in domain.CreateAccountInput
	if err := decode(w, r, &in); err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	a, err := h.svc.CreateAccount(r.Context(), in)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (h *Handler) getAccount(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	a, err := h.svc.GetAccount(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (h *Handler) listEntries(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	q := r.URL.Query()
	cursor, err := queryInt(q.Get("cursor"), "cursor")
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	limit, err := queryInt(q.Get("limit"), "limit")
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	page, err := h.svc.ListEntries(r.Context(), id, cursor, int(limit))
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handler) getTransfer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	t, err := h.svc.GetTransfer(r.Context(), id)
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

type accountAmountRequest struct {
	AccountID uuid.UUID `json:"account_id"`
	Amount    int64     `json:"amount"`
}

type transferRequest struct {
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	Amount        int64     `json:"amount"`
}

func (h *Handler) deposit(w http.ResponseWriter, r *http.Request) {
	var req accountAmountRequest
	if err := decode(w, r, &req); err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	res, err := h.svc.Deposit(r.Context(), req.AccountID, req.Amount, idempotencyKey(r))
	h.writeTransfer(w, r, res, err)
}

func (h *Handler) withdraw(w http.ResponseWriter, r *http.Request) {
	var req accountAmountRequest
	if err := decode(w, r, &req); err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	res, err := h.svc.Withdraw(r.Context(), req.AccountID, req.Amount, idempotencyKey(r))
	h.writeTransfer(w, r, res, err)
}

func (h *Handler) transfer(w http.ResponseWriter, r *http.Request) {
	var req transferRequest
	if err := decode(w, r, &req); err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	res, err := h.svc.Transfer(r.Context(), req.FromAccountID, req.ToAccountID, req.Amount, idempotencyKey(r))
	h.writeTransfer(w, r, res, err)
}

// writeTransfer answers 201 for a new transfer and 200 with the
// Idempotent-Replayed header when the same request was already executed.
func (h *Handler) writeTransfer(w http.ResponseWriter, r *http.Request, res *ledger.TransferResult, err error) {
	if err != nil {
		h.writeDomainError(w, r, err)
		return
	}
	status := http.StatusCreated
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		status = http.StatusOK
	}
	writeJSON(w, status, res.Transfer)
}

func idempotencyKey(r *http.Request) string {
	return r.Header.Get("Idempotency-Key")
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, &domain.ValidationError{Field: "id", Message: "must be a UUID"}
	}
	return id, nil
}

func queryInt(s, field string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, &domain.ValidationError{Field: field, Message: "must be a non-negative integer"}
	}
	return v, nil
}
