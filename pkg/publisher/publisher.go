package publisher

import (
	"context"
	"github.com/silverswords/pulse/pkg/driver"
	"github.com/silverswords/pulse/pkg/message"
)

type Publisher struct {
	d driver.Driver
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg message.Msger) (err error) {
	err = p.d.Publish(topic, msg.Msg())
	return
}
