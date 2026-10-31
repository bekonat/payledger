package domain

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

var supportedCurrencies = map[string]struct{}{
	"KZT": {},
	"USD": {},
	"EUR": {},
	"RUB": {},
}

type Account struct {
	ID        uuid.UUID `json:"id"`
	OwnerID   string    `json:"owner_id"`
	Currency  string    `json:"currency"`
	Balance   int64     `json:"balance"`
	System    bool      `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateAccountInput struct {
	OwnerID  string `json:"owner_id"`
	Currency string `json:"currency"`
}

func (in *CreateAccountInput) Normalize() error {
	in.OwnerID = strings.TrimSpace(in.OwnerID)
	in.Currency = strings.ToUpper(strings.TrimSpace(in.Currency))
	if in.OwnerID == "" {
		return invalid("owner_id", "must not be empty")
	}
	if len(in.OwnerID) > 64 {
		return invalid("owner_id", "must be at most 64 characters")
	}
	if in.OwnerID == SystemOwner {
		return invalid("owner_id", "is reserved")
	}
	return ValidateCurrency(in.Currency)
}

const SystemOwner = "system"

func ValidateCurrency(c string) error {
	if _, ok := supportedCurrencies[c]; !ok {
		return invalid("currency", "unsupported currency")
	}
	return nil
}
