package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/bekonat/payledger/internal/domain"
)

type errorBody struct {
	Error string `json:"error"`
	Field string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

const maxBodyBytes = 1 << 16

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return &domain.ValidationError{Field: "body", Message: "invalid JSON: " + err.Error()}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &domain.ValidationError{Field: "body", Message: "must contain a single JSON object"}
	}
	return nil
}

// writeDomainError maps domain errors to HTTP status codes in one place, so
// handlers never decide status codes for business errors themselves.
func (h *Handler) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{Error: ve.Message, Field: ve.Field})
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInsufficientFunds),
		errors.Is(err, domain.ErrCurrencyMismatch),
		errors.Is(err, domain.ErrSameAccount),
		errors.Is(err, domain.ErrSystemAccount):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		writeError(w, http.StatusConflict, err.Error())
	default:
		h.log.ErrorContext(r.Context(), "internal error", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}
