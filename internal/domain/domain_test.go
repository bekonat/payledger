package domain

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestTransferInputValidate(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	tests := []struct {
		name      string
		in        TransferInput
		wantField string
		wantErr   error
	}{
		{"valid transfer", TransferInput{Kind: KindTransfer, FromAccountID: a, ToAccountID: b, Amount: 100, IdempotencyKey: "k"}, "", nil},
		{"valid deposit", TransferInput{Kind: KindDeposit, ToAccountID: a, Amount: 1, IdempotencyKey: "k"}, "", nil},
		{"valid withdrawal", TransferInput{Kind: KindWithdrawal, FromAccountID: a, Amount: 1, IdempotencyKey: "k"}, "", nil},
		{"missing key", TransferInput{Kind: KindDeposit, ToAccountID: a, Amount: 1}, "Idempotency-Key", nil},
		{"zero amount", TransferInput{Kind: KindDeposit, ToAccountID: a, IdempotencyKey: "k"}, "amount", nil},
		{"negative amount", TransferInput{Kind: KindDeposit, ToAccountID: a, Amount: -5, IdempotencyKey: "k"}, "amount", nil},
		{"too large", TransferInput{Kind: KindDeposit, ToAccountID: a, Amount: MaxAmount + 1, IdempotencyKey: "k"}, "amount", nil},
		{"deposit without account", TransferInput{Kind: KindDeposit, Amount: 1, IdempotencyKey: "k"}, "account_id", nil},
		{"transfer without source", TransferInput{Kind: KindTransfer, ToAccountID: b, Amount: 1, IdempotencyKey: "k"}, "from_account_id", nil},
		{"same account", TransferInput{Kind: KindTransfer, FromAccountID: a, ToAccountID: a, Amount: 1, IdempotencyKey: "k"}, "", ErrSameAccount},
		{"unknown kind", TransferInput{Kind: "refund", Amount: 1, IdempotencyKey: "k"}, "kind", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.in.Validate()
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("got %v, want %v", err, tt.wantErr)
				}
				return
			}
			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Field != tt.wantField {
				t.Fatalf("got %v, want validation error on %q", err, tt.wantField)
			}
		})
	}
}

func TestCreateAccountInputNormalize(t *testing.T) {
	in := CreateAccountInput{OwnerID: "  user-1 ", Currency: " kzt"}
	if err := in.Normalize(); err != nil {
		t.Fatal(err)
	}
	if in.OwnerID != "user-1" || in.Currency != "KZT" {
		t.Fatalf("not normalized: %+v", in)
	}

	for _, bad := range []CreateAccountInput{
		{OwnerID: "", Currency: "KZT"},
		{OwnerID: "system", Currency: "KZT"},
		{OwnerID: "u", Currency: "BTC"},
	} {
		if err := bad.Normalize(); err == nil {
			t.Errorf("expected error for %+v", bad)
		}
	}
}
