package grpcapi

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ledgerv1 "github.com/bekonat/payledger/gen/ledger/v1"
	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/ledger"
)

type fakeService struct {
	err   error
	panic bool
}

func (f *fakeService) CreateAccount(_ context.Context, in domain.CreateAccountInput) (*domain.Account, error) {
	if err := in.Normalize(); err != nil {
		return nil, err
	}
	return &domain.Account{ID: uuid.New(), OwnerID: in.OwnerID, Currency: in.Currency}, nil
}

func (f *fakeService) GetAccount(context.Context, uuid.UUID) (*domain.Account, error) {
	if f.panic {
		panic("boom")
	}
	return nil, f.err
}

func (f *fakeService) ListEntries(context.Context, uuid.UUID, int64, int) (*domain.EntryPage, error) {
	return &domain.EntryPage{Entries: []domain.Entry{{ID: 2, Amount: -5}, {ID: 1, Amount: 5}}, NextCursor: 1}, nil
}

func (f *fakeService) transfer() (*ledger.TransferResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &ledger.TransferResult{Transfer: &domain.Transfer{ID: uuid.New(), Kind: domain.KindTransfer, Amount: 10}, Replayed: true}, nil
}

func (f *fakeService) Deposit(context.Context, uuid.UUID, int64, string) (*ledger.TransferResult, error) {
	return f.transfer()
}

func (f *fakeService) Withdraw(context.Context, uuid.UUID, int64, string) (*ledger.TransferResult, error) {
	return f.transfer()
}

func (f *fakeService) Transfer(context.Context, uuid.UUID, uuid.UUID, int64, string) (*ledger.TransferResult, error) {
	return f.transfer()
}

// client starts a real gRPC server over an in-memory listener, so the test
// covers interceptors and status encoding without opening a TCP port.
func client(t *testing.T, svc LedgerService) ledgerv1.LedgerServiceClient {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(UnaryInterceptor(log)))
	ledgerv1.RegisterLedgerServiceServer(srv, NewServer(svc, log))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

func TestStatusCodes(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		svc  *fakeService
		call func(c ledgerv1.LedgerServiceClient) error
		want codes.Code
	}{
		{"bad uuid", &fakeService{}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: "x"})
			return err
		}, codes.InvalidArgument},
		{"not found", &fakeService{err: domain.ErrNotFound}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: uuid.NewString()})
			return err
		}, codes.NotFound},
		{"insufficient funds", &fakeService{err: domain.ErrInsufficientFunds}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.Withdraw(ctx, &ledgerv1.WithdrawRequest{AccountId: uuid.NewString(), Amount: 1, IdempotencyKey: "k"})
			return err
		}, codes.FailedPrecondition},
		{"key reused", &fakeService{err: domain.ErrIdempotencyMismatch}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: uuid.NewString(), Amount: 1, IdempotencyKey: "k"})
			return err
		}, codes.AlreadyExists},
		{"panic", &fakeService{panic: true}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: uuid.NewString()})
			return err
		}, codes.Internal},
		{"validation", &fakeService{}, func(c ledgerv1.LedgerServiceClient) error {
			_, err := c.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{OwnerId: "u", Currency: "XXX"})
			return err
		}, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call(client(t, tt.svc))
			if got := status.Code(err); got != tt.want {
				t.Fatalf("code = %v, want %v (%v)", got, tt.want, err)
			}
		})
	}
}

func TestTransferAndEntries(t *testing.T) {
	ctx := context.Background()
	c := client(t, &fakeService{})
	res, err := c.Transfer(ctx, &ledgerv1.TransferRequest{
		FromAccountId: uuid.NewString(), ToAccountId: uuid.NewString(), Amount: 10, IdempotencyKey: "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetReplayed() || res.GetTransfer().GetAmount() != 10 || res.GetTransfer().GetKind() != "transfer" {
		t.Fatalf("unexpected response: %v", res)
	}

	page, err := c.ListEntries(ctx, &ledgerv1.ListEntriesRequest{AccountId: uuid.NewString(), Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetEntries()) != 2 || page.GetNextCursor() != 1 {
		t.Fatalf("unexpected page: %v", page)
	}
}
