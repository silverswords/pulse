package protocol

import (
	"context"
	"github.com/silverswords/pulse/pkg/visitor"
)

type Message struct {
	Data []byte

	Topic       string
	OrderingKey string
}

func (m *Message) Do(fn visitor.DoFunc) error {
	return func() error {
		err := fn(m, context.Background(), nil)
		return err
	}()
}
