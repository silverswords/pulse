// scheduler provide an asynchronously, streaming and bundle buffered process way to pub/sub.
package scheduler

import (
	"context"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
)

type Bundler struct {
	Driver     driver.Driver
	Scheduler  *BundleScheduler
	BundleFunc func(ctx context.Context, msgs []*message.Message)
}

func NewBundler() *Bundler {
	return &Bundler{}
}

func (b *Bundler) Publish() {
	//	start()

	//	just add to queue

	//	waiting for bundle and handle

}

func (b *Bundler) Subscribe() {
	//	Subscribe something

	// waiting for bundle and handle.
}

type Single struct {
	Driver     driver.Driver
	Scheduler  *Scheduler
	HandleFunc func(ctx context.Context, m *message.Message)
}
