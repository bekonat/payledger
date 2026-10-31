package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
	"github.com/bekonat/payledger/internal/observability"
)

type fakeService struct {
	transferErr error
	replayed    bool
	gotKey      string
}

func (f *fakeService) CreateAccount(_ context.Context, in domain.CreateAccountInput) (*domain.Account, error) {
	if err := in.Normalize(); err != nil {
		return nil, err
	}
	return &domain.Account{ID: uuid.New(), OwnerID: in.OwnerID, Currency: in.Currency}, nil
}

func (f *fakeService) GetAccount(context.Context, uuid.UUID) (*domain.Account, error) {
	return nil, domain.ErrNotFound
}

func (f *fakeService) ListEntries(context.Context, uuid.UUID, int64, int) (*domain.EntryPage, error) {
	return &domain.EntryPage{}, nil
}

func (f *fakeService) GetTransfer(context.Context, uuid.UUID) (*domain.Transfer, error) {
	return nil, errors.New("connection reset")
}

func (f *fakeService) result(key string) (*ledger.TransferResult, error) {
	f.gotKey = key
	if f.transferErr != nil {
		return nil, f.transferErr
	}
	return &ledger.TransferResult{Transfer: &domain.Transfer{ID: uuid.New()}, Replayed: f.replayed}, nil
}

func (f *fakeService) Deposit(_ context.Context, _ uuid.UUID, _ int64, key string) (*ledger.TransferResult, error) {
	return f.result(key)
}

func (f *fakeService) Withdraw(_ context.Context, _ uuid.UUID, _ int64, key string) (*ledger.TransferResult, error) {
	return f.result(key)
}

func (f *fakeService) Transfer(_ context.Context, _, _ uuid.UUID, _ int64, key string) (*ledger.TransferResult, error) {
	return f.result(key)
}

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

type fakeLimiter struct {
	allow bool
	err   error
}

func (l fakeLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return l.allow, 1500 * time.Millisecond, l.err
}

func newTestHandler(svc LedgerService, lim Limiter) http.Handler {
	return NewHandler(svc, okPinger{}, lim, slog.New(slog.NewTextHandler(io.Discard, nil))).Routes()
}

func do(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestErrorMapping(t *testing.T) {
	id := uuid.NewString()
	transfer := `{"from_account_id":"` + id + `","to_account_id":"` + uuid.NewString() + `","amount":100}`
	tests := []struct {
		name   string
		err    error
		method string
		path   string
		body   string
		want   int
	}{
		{"created", nil, "POST", "/api/v1/transfers", transfer, http.StatusCreated},
		{"insufficient funds", domain.ErrInsufficientFunds, "POST", "/api/v1/transfers", transfer, http.StatusUnprocessableEntity},
		{"key reused", domain.ErrIdempotencyMismatch, "POST", "/api/v1/transfers", transfer, http.StatusConflict},
		{"account missing", domain.ErrNotFound, "POST", "/api/v1/transfers", transfer, http.StatusNotFound},
		{"unknown field", nil, "POST", "/api/v1/transfers", `{"amount":1,"extra":true}`, http.StatusUnprocessableEntity},
		{"trailing data", nil, "POST", "/api/v1/deposits", `{"amount":1}{}`, http.StatusUnprocessableEntity},
		{"bad uuid in path", nil, "GET", "/api/v1/accounts/nope", "", http.StatusUnprocessableEntity},
		{"not found", nil, "GET", "/api/v1/accounts/" + id, "", http.StatusNotFound},
		{"internal error is hidden", nil, "GET", "/api/v1/transfers/" + id, "", http.StatusInternalServerError},
		{"bad limit", nil, "GET", "/api/v1/accounts/" + id + "/entries?limit=-1", "", http.StatusUnprocessableEntity},
		{"wrong method", nil, "DELETE", "/api/v1/transfers", "", http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(&fakeService{transferErr: tt.err}, nil)
			rec := do(h, tt.method, tt.path, tt.body, map[string]string{"Idempotency-Key": "k"})
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.want, rec.Body)
			}
			if rec.Code == http.StatusInternalServerError && strings.Contains(rec.Body.String(), "connection reset") {
				t.Fatal("internal error details leaked to the client")
			}
		})
	}
}

func TestIdempotencyHeaders(t *testing.T) {
	svc := &fakeService{replayed: true}
	h := newTestHandler(svc, nil)
	rec := do(h, "POST", "/api/v1/deposits", `{"account_id":"`+uuid.NewString()+`","amount":5}`,
		map[string]string{"Idempotency-Key": "abc-123"})
	if rec.Code != http.StatusOK || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: status=%d header=%q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
	if svc.gotKey != "abc-123" {
		t.Fatalf("idempotency key not passed to service: %q", svc.gotKey)
	}
}

func TestValidationErrorNamesField(t *testing.T) {
	h := newTestHandler(&fakeService{}, nil)
	rec := do(h, "POST", "/api/v1/accounts", `{"owner_id":"u1","currency":"BTC"}`, nil)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"field":"currency"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestRateLimit(t *testing.T) {
	t.Run("limited", func(t *testing.T) {
		h := newTestHandler(&fakeService{}, fakeLimiter{allow: false})
		rec := do(h, "GET", "/api/v1/accounts/"+uuid.NewString(), "", nil)
		if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
			t.Fatalf("status=%d retry-after=%q", rec.Code, rec.Header().Get("Retry-After"))
		}
	})
	t.Run("fail open", func(t *testing.T) {
		h := newTestHandler(&fakeService{}, fakeLimiter{err: errors.New("redis down")})
		rec := do(h, "GET", "/api/v1/accounts/"+uuid.NewString(), "", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("request must reach the handler when redis is down, got %d", rec.Code)
		}
	})
	t.Run("health is not limited", func(t *testing.T) {
		h := newTestHandler(&fakeService{}, fakeLimiter{allow: false})
		if rec := do(h, "GET", "/healthz", "", nil); rec.Code != http.StatusOK {
			t.Fatalf("healthz got %d", rec.Code)
		}
	})
}

func TestMetricsUseRoutePattern(t *testing.T) {
	h := newTestHandler(&fakeService{}, nil)
	before := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("GET /api/v1/accounts/{id}", "GET", "404"))
	do(h, "GET", "/api/v1/accounts/"+uuid.NewString(), "", nil)
	do(h, "GET", "/api/v1/accounts/"+uuid.NewString(), "", nil)
	after := testutil.ToFloat64(observability.HTTPRequests.WithLabelValues("GET /api/v1/accounts/{id}", "GET", "404"))
	if after-before != 2 {
		t.Fatalf("two requests to different ids must share one series, delta = %v", after-before)
	}
}

func TestRequestIDIsEchoed(t *testing.T) {
	h := newTestHandler(&fakeService{}, nil)
	rec := do(h, "GET", "/healthz", "", map[string]string{"X-Request-ID": "req-42"})
	if rec.Header().Get("X-Request-ID") != "req-42" {
		t.Fatalf("X-Request-ID = %q", rec.Header().Get("X-Request-ID"))
	}
}
