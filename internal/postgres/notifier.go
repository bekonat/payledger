package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bekonat/payledger/internal/notifier"
)

type NotificationStore struct {
	pool *pgxpool.Pool
}

func NewNotificationStore(pool *pgxpool.Pool) *NotificationStore {
	return &NotificationStore{pool: pool}
}

// ProcessOnce records the event id and its notifications in one transaction.
// If the id is already in the inbox the event is a redelivery and nothing
// is written.
func (s *NotificationStore) ProcessOnce(ctx context.Context, eventID string, ns []notifier.Notification) (bool, error) {
	var processed bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO notifier.processed_events (event_id) VALUES ($1)
			ON CONFLICT DO NOTHING`, eventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		processed = true
		for _, n := range ns {
			_, err := tx.Exec(ctx, `
				INSERT INTO notifier.notifications (event_id, owner_id, account_id, message)
				VALUES ($1, $2, $3, $4)`, n.EventID, n.OwnerID, n.AccountID, n.Message)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return processed, err
}

func (s *NotificationStore) List(ctx context.Context, ownerID string, limit int) ([]notifier.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id::text, owner_id, account_id::text, message, created_at
		FROM notifier.notifications
		WHERE owner_id = $1
		ORDER BY id DESC
		LIMIT $2`, ownerID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (notifier.Notification, error) {
		var n notifier.Notification
		err := row.Scan(&n.ID, &n.EventID, &n.OwnerID, &n.AccountID, &n.Message, &n.CreatedAt)
		return n, err
	})
}

func (s *NotificationStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
