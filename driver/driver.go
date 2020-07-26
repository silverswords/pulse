package driver

import (
	"context"
	"fmt"
	"github.com/silverswords/whisper/message"
)

type pubSubRegistry struct {
	buses map[string]func() Driver
}

func (r *pubSubRegistry) Register(name string, factory func() Driver) {
	r.buses[createFullName(name)] = factory
}

// Create instantiates a pub/sub based on `name`.
func (p *pubSubRegistry) Create(name string) (Driver, error) {
	if method, ok := p.buses[name]; ok {
		return method(), nil
	}
	return nil, fmt.Errorf("couldn't find message bus %s", name)
}

type Metadata struct {
	Properties map[string]string
}

func createFullName(name string) string { return fmt.Sprintf("pubsub.%s", name) }

type Initer interface {
	Init() error
}

type Driver interface {
	Initer
	Publisher
	Subscriber
	Closer
}

type Publisher interface {
	Publish(in *message.Message) error
}

// Subscriber is a blocking method
// should be cancel() with ctx or call Driver.Close() to close all the subscribers.
type Subscriber interface {
	Subscribe(topic string, handler func(msg *message.Message) error) (Closer, error)
}

// Closer is the common interface for things that can be closed.
// After invoking Close(ctx), you cannot reuse the object you closed.
type Closer interface {
	Close() error
}

type Client interface {
	Sender

	// startReceive start to handle msg := client.Receive(). with fn function.
	StartReceive(ctx context.Context, fn interface{}) error
}

type Requests interface {
	Request(ctx context.Context, request *message.Message) (resp *message.Message, err error)
}

type SenderCloser interface {
	Sender
	Closer
}

type Sender interface {
	Send(ctx context.Context, msg *message.Message) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg *message.Message, err error)
}
