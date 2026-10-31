package domain

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrSameAccount         = errors.New("source and destination accounts are the same")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
	ErrSystemAccount       = errors.New("operation is not allowed on a system account")
)

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func invalid(field, msg string) error {
	return &ValidationError{Field: field, Message: msg}
}
