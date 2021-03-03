package publisher

import (
	"context"
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

func (p *PublishScheduler) Publish(actor message.Actor, ctx context.Context, err error) error {
	return actor.Do(p.Publisher.Publish)
}
