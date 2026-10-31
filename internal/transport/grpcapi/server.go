package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/bekonat/payledger/gen/ledger/v1"
	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
	"github.com/bekonat/payledger/internal/observability"
)

// LedgerService is declared here as well as in httpapi: each consumer owns
// the interface it needs, and the two transports stay independent.
type LedgerService interface {
	CreateAccount(ctx context.Context, in domain.CreateAccountInput) (*domain.Account, error)
	GetAccount(ctx context.Context, id uuid.UUID) (*domain.Account, error)
	ListEntries(ctx context.Context, accountID uuid.UUID, cursor int64, limit int) (*domain.EntryPage, error)
	Deposit(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
	Withdraw(ctx context.Context, accountID uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
	Transfer(ctx context.Context, from, to uuid.UUID, amount int64, key string) (*ledger.TransferResult, error)
}

type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc LedgerService
	log *slog.Logger
}

func NewServer(svc LedgerService, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log}
}

func (s *Server) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (*ledgerv1.Account, error) {
	a, err := s.svc.CreateAccount(ctx, domain.CreateAccountInput{OwnerID: req.GetOwnerId(), Currency: req.GetCurrency()})
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return accountToProto(a), nil
}

func (s *Server) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.Account, error) {
	id, err := parseID("id", req.GetId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	a, err := s.svc.GetAccount(ctx, id)
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return accountToProto(a), nil
}

func (s *Server) ListEntries(ctx context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	id, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	page, err := s.svc.ListEntries(ctx, id, req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	resp := &ledgerv1.ListEntriesResponse{NextCursor: page.NextCursor}
	for _, e := range page.Entries {
		resp.Entries = append(resp.Entries, &ledgerv1.Entry{
			Id:           e.ID,
			TransferId:   e.TransferID.String(),
			AccountId:    e.AccountID.String(),
			Amount:       e.Amount,
			BalanceAfter: e.BalanceAfter,
			CreatedAt:    timestamppb.New(e.CreatedAt),
		})
	}
	return resp, nil
}

func (s *Server) Deposit(ctx context.Context, req *ledgerv1.DepositRequest) (*ledgerv1.TransferResponse, error) {
	id, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	res, err := s.svc.Deposit(ctx, id, req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return transferToProto(res), nil
}

func (s *Server) Withdraw(ctx context.Context, req *ledgerv1.WithdrawRequest) (*ledgerv1.TransferResponse, error) {
	id, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	res, err := s.svc.Withdraw(ctx, id, req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return transferToProto(res), nil
}

func (s *Server) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.TransferResponse, error) {
	from, err := parseID("from_account_id", req.GetFromAccountId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	to, err := parseID("to_account_id", req.GetToAccountId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	res, err := s.svc.Transfer(ctx, from, to, req.GetAmount(), req.GetIdempotencyKey())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	return transferToProto(res), nil
}

func transferToProto(res *ledger.TransferResult) *ledgerv1.TransferResponse {
	t := res.Transfer
	return &ledgerv1.TransferResponse{
		Replayed: res.Replayed,
		Transfer: &ledgerv1.Transfer{
			Id:            t.ID.String(),
			Kind:          string(t.Kind),
			FromAccountId: t.FromAccountID.String(),
			ToAccountId:   t.ToAccountID.String(),
			Amount:        t.Amount,
			Currency:      t.Currency,
			CreatedAt:     timestamppb.New(t.CreatedAt),
		},
	}
}

func accountToProto(a *domain.Account) *ledgerv1.Account {
	return &ledgerv1.Account{
		Id:        a.ID.String(),
		OwnerId:   a.OwnerID,
		Currency:  a.Currency,
		Balance:   a.Balance,
		CreatedAt: timestamppb.New(a.CreatedAt),
		UpdatedAt: timestamppb.New(a.UpdatedAt),
	}
}

func parseID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, &domain.ValidationError{Field: field, Message: "must be a UUID"}
	}
	return id, nil
}

// toStatus is the gRPC twin of the HTTP error mapping. FailedPrecondition is
// used for business rule violations: the request is valid, but the system
// state does not allow it (for example, not enough money).
func (s *Server) toStatus(ctx context.Context, err error) error {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		return status.Error(codes.InvalidArgument, ve.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, domain.ErrInsufficientFunds),
		errors.Is(err, domain.ErrCurrencyMismatch),
		errors.Is(err, domain.ErrSystemAccount):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrSameAccount):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		s.log.ErrorContext(ctx, "internal error", slog.String("error", err.Error()))
		return status.Error(codes.Internal, "internal error")
	}
}

// UnaryInterceptor recovers panics, records metrics and logs every call.
func UnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		defer func() {
			if v := recover(); v != nil {
				log.ErrorContext(ctx, "panic recovered", slog.Any("panic", v), slog.String("stack", string(debug.Stack())))
				err = status.Error(codes.Internal, "internal error")
			}
			code := status.Code(err)
			observability.GRPCRequests.WithLabelValues(info.FullMethod, code.String()).Inc()
			observability.GRPCDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
			attrs := []any{
				slog.String("method", info.FullMethod),
				slog.String("code", code.String()),
				slog.Duration("duration", time.Since(start)),
			}
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if ids := md.Get("x-request-id"); len(ids) > 0 {
					attrs = append(attrs, slog.String("request_id", ids[0]))
				}
			}
			if code == codes.Internal {
				log.ErrorContext(ctx, "grpc request", attrs...)
			} else {
				log.InfoContext(ctx, "grpc request", attrs...)
			}
		}()
		return handler(ctx, req)
	}
}
