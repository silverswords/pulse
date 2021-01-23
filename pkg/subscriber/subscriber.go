package subscriber

import (
	"context"
	"github.com/silverswords/pulse/pkg/adapter"
	"github.com/silverswords/pulse/pkg/driver"
	"github.com/silverswords/pulse/pkg/message"
)

type Subscriber struct {
	Driver driver.Driver
	Codec  adapter.Codec
}

func (s *Subscriber) Subscribe(topic string, handler func(ctx context.Context, m *message.Msger)) (driver.Closer, error) {
	return nil, nil
}
