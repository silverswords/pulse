package publisher

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/adapter"
	"github.com/silverswords/pulse/pkg/driver"
)

type Publisher struct {
	d driver.Driver
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg interface{}, codec adapter.Codec) (err error) {
	if b, err := codec.Marshal(msg); err != nil {
		return p.d.Publish(topic, b)
	}
	return errors.New("could not marshal message")
}
