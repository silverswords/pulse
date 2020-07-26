package whisper

import (
	"context"
	"fmt"
	"github.com/silverswords/whisper/message"
)

type pubSubRegistry struct {
	buses map[string]func() Driver
}

func (r *pubSubRegistry)Register(name string, factory func() Driver) {
	r.buses[createFullName(name)] = factory
}

// Create instantiates a pub/sub based on `name`.
func (p *pubSubRegistry) Create(name string) (Driver, error) {
	if method, ok := p.buses[name]; ok {
		return method(), nil
	}
	return nil, fmt.Errorf("couldn't find message bus %s", name)
}

func createFullName(name string) string{return fmt.Sprintf("pubsub.%s", name)}


type Publisher interface {
	Publish(ctx context.Context, in *message.Message) error
}
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler func(msg *message.Message) error) error
}

type Client interface {
	Sender

	// startReceive start to handle msg := client.Receive(). with fn function.
	StartReceive(ctx context.Context, fn interface{}) error
}
type Sender interface {
	Send(ctx context.Context, msg *message.Message) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg *message.Message, err error)
}

type Requests interface {
	Request(ctx context.Context, request *message.Message) (resp *message.Message, err error)
}
type SenderCloser interface {
	Sender
	Closer
}

type Initer interface {
	Init(metadata interface{}) error
}

type Metadata struct {
	Properties map[string]string
}


// Closer is the common interface for things that can be closed.
// After invoking Close(ctx), you cannot reuse the object you closed.
type Closer interface {
	Close(ctx context.Context) error
}

type Driver interface {
	Sender
	Receiver
	Closer
}


type ReceiverCloser interface {
	Initer
	Receiver
	Closer
}

// Opener is the common interface for things that need to be opened.
type Opener interface {
	// Open is a blocking call and ctx is used to stop the Inbound message Receiver/Responder.
	// Closing the context won't close the Receiver/Responder, aka it won't invoke Close(ctx).
	Open(ctx context.Context) error
}