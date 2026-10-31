package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bekonat/payledger/internal/outbox"
)

type OutboxSource struct {
	pool *pgxpool.Pool
}

func NewOutboxSource(pool *pgxpool.Pool) *OutboxSource {
	return &OutboxSource{pool: pool}
}

// Claim locks up to limit unpublished rows with FOR UPDATE SKIP LOCKED.
// Several relay replicas can run at once: each one skips the rows another
// replica is publishing right now instead of waiting for them.
func (s *OutboxSource) Claim(ctx context.Context, limit int, fn func(context.Context, []outbox.Message) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, topic, message_key, payload, headers, created_at
			FROM outbox
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Message, error) {
			var m outbox.Message
			err := row.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload, &m.Headers, &m.CreatedAt)
			return m, err
		})
		if err != nil {
			return err
		}
		n = len(msgs)
		if n == 0 {
			return nil
		}
		if err := fn(ctx, msgs); err != nil {
			return err
		}
		ids := make([]int64, n)
		for i, m := range msgs {
			ids[i] = m.ID
		}
		_, err = tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
		return err
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *OutboxSource) OldestUnpublished(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8
		FROM outbox WHERE published_at IS NULL`).Scan(&seconds)
	return time.Duration(seconds * float64(time.Second)), err
}

// DeletePublished removes old published rows in small chunks so cleanup never
// holds long locks or produces one huge transaction.
func (s *OutboxSource) DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error) {
	var total int64
	for {
		tag, err := s.pool.Exec(ctx, `
			DELETE FROM outbox WHERE id IN (
				SELECT id FROM outbox
				WHERE published_at < now() - make_interval(secs => $1)
				LIMIT 1000
			)`, olderThan.Seconds())
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 1000 {
			return total, nil
		}
	}
}
