package publisher

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/adapter"
	"github.com/silverswords/pulse/pkg/driver"
	"github.com/silverswords/pulse/pkg/message"
)

type PublishScheduler struct {
	driver.Publisher
	scheduler *PublishScheduler
}

func NewPublishScheduler(publisher driver.Publisher, scheduler *PublishScheduler) *PublishScheduler {
	return &PublishScheduler{Publisher: publisher, scheduler: scheduler}
}

func (p *PublishScheduler) Publish(ctx context.Context, msg message.Message, codec adapter.Codec) (err error) {
	if b, err := codec.Marshal(msg); err != nil {
		return p.Publisher.Publish(topic, b)
	}
	return errors.New("could not marshal message")
}
