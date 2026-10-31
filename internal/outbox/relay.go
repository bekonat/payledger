package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/bekonat/payledger/internal/kafka"
	"github.com/bekonat/payledger/internal/observability"
)

var tracer = otel.Tracer("github.com/bekonat/payledger/internal/outbox")

type Message struct {
	ID        int64
	Topic     string
	Key       string
	Payload   []byte
	Headers   map[string]string
	CreatedAt time.Time
}

// Source hands out batches of unpublished messages. Claim keeps the batch
// locked while fn runs and marks it published only if fn returns nil.
type Source interface {
	Claim(ctx context.Context, limit int, fn func(ctx context.Context, msgs []Message) error) (int, error)
	OldestUnpublished(ctx context.Context) (time.Duration, error)
	DeletePublished(ctx context.Context, olderThan time.Duration) (int64, error)
}

type Publisher interface {
	Publish(ctx context.Context, msgs []kafka.Message) error
}

type Config struct {
	BatchSize    int
	PollInterval time.Duration
	MaxBackoff   time.Duration
	Retention    time.Duration
}

type Relay struct {
	src Source
	pub Publisher
	cfg Config
	log *slog.Logger
}

func NewRelay(src Source, pub Publisher, cfg Config, log *slog.Logger) *Relay {
	return &Relay{src: src, pub: pub, cfg: cfg, log: log}
}

// Run publishes until ctx is cancelled. A full batch means there is probably
// more work, so the next batch starts right away; otherwise the relay sleeps
// for PollInterval. Failures back off exponentially up to MaxBackoff.
func (r *Relay) Run(ctx context.Context) error {
	backoff := r.cfg.PollInterval
	lastMaintenance := time.Time{}
	for {
		if time.Since(lastMaintenance) > time.Minute {
			r.maintenance(ctx)
			lastMaintenance = time.Now()
		}

		n, err := r.src.Claim(ctx, r.cfg.BatchSize, r.publish)
		wait := r.cfg.PollInterval
		switch {
		case errors.Is(err, context.Canceled) && ctx.Err() != nil:
			return nil
		case err != nil:
			observability.OutboxErrors.Inc()
			r.log.ErrorContext(ctx, "outbox batch failed", slog.String("error", err.Error()), slog.Duration("retry_in", backoff))
			wait = backoff
			backoff = min(backoff*2, r.cfg.MaxBackoff)
		case n == r.cfg.BatchSize:
			backoff = r.cfg.PollInterval
			wait = 0
		default:
			backoff = r.cfg.PollInterval
		}

		if wait > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
			}
		} else if ctx.Err() != nil {
			return nil
		}
	}
}

func (r *Relay) publish(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	links := make([]trace.Link, 0, len(msgs))
	out := make([]kafka.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafka.Message{Topic: m.Topic, Key: m.Key, Value: m.Payload, Headers: m.Headers}
		parent := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.Headers))
		if sc := trace.SpanContextFromContext(parent); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	ctx, span := tracer.Start(ctx, "outbox.publish",
		trace.WithLinks(links...),
		trace.WithAttributes(attribute.Int("outbox.batch_size", len(msgs))))
	defer span.End()

	if err := r.pub.Publish(ctx, out); err != nil {
		span.RecordError(err)
		return err
	}
	observability.OutboxPublished.Add(float64(len(msgs)))
	observability.OutboxBatchSize.Observe(float64(len(msgs)))
	return nil
}

func (r *Relay) maintenance(ctx context.Context) {
	if lag, err := r.src.OldestUnpublished(ctx); err == nil {
		observability.OutboxLag.Set(lag.Seconds())
	}
	if r.cfg.Retention <= 0 {
		return
	}
	n, err := r.src.DeletePublished(ctx, r.cfg.Retention)
	if err != nil {
		r.log.WarnContext(ctx, "outbox cleanup failed", slog.String("error", err.Error()))
		return
	}
	if n > 0 {
		r.log.InfoContext(ctx, "outbox cleanup", slog.Int64("deleted", n))
	}
}
