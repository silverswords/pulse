package endpoint

import (
	"context"
	"github.com/silverswords/pulse/pkg/adapter"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"github.com/silverswords/pulse/pkg/scheduler"
)

type Endpoint struct {
	Driver       driver.Driver
	Codec        adapter.Codec
	publishqueue scheduler.BundleScheduler

	drivers map[string]driver.Driver
}

func (ep Endpoint) Publish(r *driver.PublishRequest, ctx context.Context, err error) error {
	panic("implement me")
}

func (ep Endpoint) Subscribe(ctx context.Context, r *driver.SubscribeRequest, handler func(message *message.Message, ctx context.Context, err error) error) (driver.Subscription, error) {
	panic("implement me")
}

func (ep Endpoint) Close() error {
	panic("implement me")
}

func NewPulse() {
	return
}

func (ep *Endpoint) Init() {

}
