package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Message is a broker-agnostic message, so the outbox and notifier packages
// can be tested without kafka-go types.
type Message struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Partition int
	Offset    int64

	raw kafkago.Message
}

type Writer struct {
	w *kafkago.Writer
}

func NewWriter(brokers []string) *Writer {
	return &Writer{w: &kafkago.Writer{
		Addr: kafkago.TCP(brokers...),
		// Hash keeps all messages with the same key in one partition, which
		// is what gives Kafka its per-key ordering.
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}}
}

func (w *Writer) Publish(ctx context.Context, msgs []Message) error {
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafkago.Message{Topic: m.Topic, Key: []byte(m.Key), Value: m.Value, Headers: toHeaders(m.Headers)}
	}
	return w.w.WriteMessages(ctx, out...)
}

func (w *Writer) Close() error { return w.w.Close() }

type Reader struct {
	r *kafkago.Reader
}

func NewReader(brokers []string, topic, group string) *Reader {
	return &Reader{r: kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     group,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafkago.FirstOffset,
		// CommitInterval 0 means CommitMessages is synchronous: an offset is
		// committed only after the message is fully processed.
		CommitInterval: 0,
	})}
}

func (r *Reader) Fetch(ctx context.Context) (Message, error) {
	m, err := r.r.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	headers := make(map[string]string, len(m.Headers))
	for _, h := range m.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Message{
		Topic: m.Topic, Key: string(m.Key), Value: m.Value, Headers: headers,
		Partition: m.Partition, Offset: m.Offset, raw: m,
	}, nil
}

func (r *Reader) Commit(ctx context.Context, m Message) error {
	return r.r.CommitMessages(ctx, m.raw)
}

func (r *Reader) Close() error { return r.r.Close() }

func toHeaders(h map[string]string) []kafkago.Header {
	out := make([]kafkago.Header, 0, len(h))
	for k, v := range h {
		out = append(out, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return out
}

// EnsureTopics creates topics that do not exist yet. It waits for the broker
// to come up, which matters when everything starts at once in docker compose.
func EnsureTopics(ctx context.Context, broker string, partitions int, topics ...string) error {
	var conn *kafkago.Conn
	var err error
	for attempt := 0; ; attempt++ {
		conn, err = kafkago.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
		if attempt == 30 {
			return fmt.Errorf("dial kafka: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := kafkago.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()

	configs := make([]kafkago.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafkago.TopicConfig{Topic: t, NumPartitions: partitions, ReplicationFactor: 1}
	}
	err = cc.CreateTopics(configs...)
	if errors.Is(err, kafkago.TopicAlreadyExists) {
		return nil
	}
	return err
}
