package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/bekonat/payledger/internal/domain"
	"github.com/bekonat/payledger/internal/kafka"
	"github.com/bekonat/payledger/internal/observability"
)

var tracer = otel.Tracer("github.com/bekonat/payledger/internal/notifier")

type Reader interface {
	Fetch(ctx context.Context) (kafka.Message, error)
	Commit(ctx context.Context, m kafka.Message) error
}

type Publisher interface {
	Publish(ctx context.Context, msgs []kafka.Message) error
}

type Notification struct {
	ID        int64     `json:"id"`
	EventID   string    `json:"event_id"`
	OwnerID   string    `json:"owner_id"`
	AccountID string    `json:"account_id"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

// Store saves notifications exactly once per event id. ProcessOnce returns
// false without calling the insert when the event was already processed.
type Store interface {
	ProcessOnce(ctx context.Context, eventID string, notifications []Notification) (bool, error)
	List(ctx context.Context, ownerID string, limit int) ([]Notification, error)
}

type Config struct {
	MaxAttempts int
	Backoff     time.Duration
	DLQTopic    string
}

// Consumer gives at-least-once delivery from Kafka and turns it into
// effectively-once processing with the inbox table. Offsets are committed
// only after a message is stored or sent to the dead letter topic.
type Consumer struct {
	reader Reader
	dlq    Publisher
	store  Store
	cfg    Config
	log    *slog.Logger
}

func NewConsumer(reader Reader, dlq Publisher, store Store, cfg Config, log *slog.Logger) *Consumer {
	return &Consumer{reader: reader, dlq: dlq, store: store, cfg: cfg, log: log}
}

var errPermanent = errors.New("permanent failure")

func (c *Consumer) Run(ctx context.Context) error {
	for {
		m, err := c.reader.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}
		if err := c.handleWithRetry(ctx, m); err != nil {
			if ctx.Err() != nil {
				// Shutting down mid-retry: do not commit, the message will be
				// redelivered to whoever owns the partition next.
				return nil
			}
			if err := c.deadLetter(ctx, m, err); err != nil {
				return fmt.Errorf("dead letter: %w", err)
			}
		}
		if err := c.reader.Commit(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("commit: %w", err)
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, m kafka.Message) error {
	backoff := c.cfg.Backoff
	var err error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		err = c.Handle(ctx, m)
		if err == nil || errors.Is(err, errPermanent) {
			return err
		}
		if attempt == c.cfg.MaxAttempts {
			break
		}
		observability.ConsumerMessages.WithLabelValues("retried").Inc()
		c.log.WarnContext(ctx, "retrying message",
			slog.Int("attempt", attempt), slog.Int64("offset", m.Offset), slog.String("error", err.Error()))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func (c *Consumer) Handle(ctx context.Context, m kafka.Message) error {
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.Headers))
	ctx, span := tracer.Start(ctx, "notifier.handle", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.destination.name", m.Topic),
			attribute.Int("messaging.kafka.partition", m.Partition),
			attribute.Int64("messaging.kafka.offset", m.Offset),
		))
	defer span.End()

	var ev domain.TransferEvent
	if err := json.Unmarshal(m.Value, &ev); err != nil {
		return fmt.Errorf("%w: decode event: %v", errPermanent, err)
	}
	if ev.EventID == uuid.Nil {
		return fmt.Errorf("%w: event without id", errPermanent)
	}

	processed, err := c.store.ProcessOnce(ctx, ev.EventID.String(), BuildNotifications(ev))
	if err != nil {
		span.RecordError(err)
		return err
	}
	if processed {
		observability.ConsumerMessages.WithLabelValues("processed").Inc()
	} else {
		observability.ConsumerMessages.WithLabelValues("duplicate").Inc()
		c.log.InfoContext(ctx, "duplicate event skipped", slog.String("event_id", ev.EventID.String()))
	}
	return nil
}

func (c *Consumer) deadLetter(ctx context.Context, m kafka.Message, cause error) error {
	headers := make(map[string]string, len(m.Headers)+3)
	for k, v := range m.Headers {
		headers[k] = v
	}
	headers["dlq_error"] = cause.Error()
	headers["dlq_source_topic"] = m.Topic
	headers["dlq_source_offset"] = fmt.Sprint(m.Offset)
	observability.ConsumerMessages.WithLabelValues("dead_lettered").Inc()
	c.log.ErrorContext(ctx, "message sent to dead letter topic",
		slog.Int64("offset", m.Offset), slog.String("error", cause.Error()))
	return c.dlq.Publish(ctx, []kafka.Message{{Topic: c.cfg.DLQTopic, Key: m.Key, Value: m.Value, Headers: headers}})
}

// BuildNotifications turns one transfer into a message for every human party.
// System accounts stand for the outside world and get no notifications.
func BuildNotifications(ev domain.TransferEvent) []Notification {
	amount := FormatAmount(ev.Amount, ev.Currency)
	var out []Notification
	add := func(p domain.EventParty, text string) {
		if p.OwnerID == domain.SystemOwner {
			return
		}
		out = append(out, Notification{
			EventID:   ev.EventID.String(),
			OwnerID:   p.OwnerID,
			AccountID: p.AccountID.String(),
			Message:   fmt.Sprintf("%s. Balance: %s", text, FormatAmount(p.BalanceAfter, ev.Currency)),
		})
	}
	switch ev.Kind {
	case domain.KindDeposit:
		add(ev.To, "Deposit +"+amount)
	case domain.KindWithdrawal:
		add(ev.From, "Withdrawal -"+amount)
	default:
		add(ev.From, fmt.Sprintf("Transfer -%s to %s", amount, ev.To.OwnerID))
		add(ev.To, fmt.Sprintf("Transfer +%s from %s", amount, ev.From.OwnerID))
	}
	return out
}

// FormatAmount prints minor units (tiyn, cents) as a decimal amount.
func FormatAmount(minor int64, currency string) string {
	sign := ""
	if minor < 0 {
		sign = "-"
		minor = -minor
	}
	return fmt.Sprintf("%s%d.%02d %s", sign, minor/100, minor%100, currency)
}
