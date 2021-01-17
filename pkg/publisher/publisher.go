package publisher

import (
	"context"
	"github.com/silverswords/pulse/pkg/driver"
)

type Publisher struct {
	driver.Driver
}

func (p *Publisher) Publish(ctx context.Context, message interface{}) {

}
