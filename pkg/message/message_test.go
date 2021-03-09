package message_test

import (
	"context"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"log"
)

func ExampleActor() {
	var m message.Message
	// warning: this publisher only pub message to console, so example does not work in real world.
	var p = ExamplePublisher{&ExampleImplPublisher{}}

	err := m.Do(p.Publish)
	log.Println("err is ", err)
}

type ExamplePublisher struct {
	driver.Publisher
}

type ExampleImplPublisher struct{}

// Publish: warning: this publisher only pub message to console, so example does not work in real world.
func (e *ExampleImplPublisher) Publish(message *message.Message, ctx context.Context, err error) error {
	log.Println(ctx, *message, err)
	return nil
}
