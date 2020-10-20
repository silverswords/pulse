package topic

import (
	"context"
	"github.com/silverswords/pulse/pkg/message"
	"sync/atomic"
)

// WithWebHook would turn on the ack function.
func WithWebHook(webhook string) Option {
	return func(t *Topic) error {
		t.WebhookURL = webhook
		return nil
	}
}

// WithRequiredACK would turn on the ack function.
func WithRequiredACK() Option {
	return func(t *Topic) error {
		t.EnableAck = true
		return nil
	}
}

func WithCount() Option {
	return func(t *Topic) error {
		var count uint64
		t.endpoints = append(t.endpoints, func(ctx context.Context, m *message.CloudEventsEnvelope) error {
			atomic.AddUint64(&count, 1)
			log.Info("count: ", count)
			return nil
		})
		return nil
	}
}

// WithRequiredACK would turn on the ack function.
func WithOrdered() Option {
	return func(t *Topic) error {
		t.EnableMessageOrdering = true
		return nil
	}
}

type Option func(*Topic) error

func (t *Topic) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}
