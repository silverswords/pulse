package driver

import (
	"context"
	"fmt"
	"github.com/silverswords/whisper"
)

var Registry = pubsubRegistry{
	buses: make(map[string]func() Driver),
}

type pubsubRegistry struct {
	buses map[string]func() Driver
}

func (r *pubsubRegistry) Register(name string, factory func() Driver) {
	r.buses[createFullName(name)] = factory
}

// Create instantiates a pub/sub based on `name`.
func (p *pubsubRegistry) Create(name string) (Driver, error) {
	if method, ok := p.buses[name]; ok {
		return method(), nil
	}
	return nil, fmt.Errorf("couldn't find message bus %s", name)
}

type Metadata struct {
	Properties map[string]interface{}
}

func createFullName(name string) string { return fmt.Sprintf("pubsub.%s", name) }

type Initer interface {
	Init(Metadata) error
}

type Driver interface {
	Initer
	Publisher
	Subscriber
	Closer
}

// Publisher should realize the retry by themselves..
// like nats, it retry when conn is reconnecting, it would be in the pending queue.
type Publisher interface {
	Publish(topic string, in *whisper.Message) error
}

// Subscriber is a blocking method
// should be cancel() with ctx or call Driver.Close() to close all the subscribers.
type Subscriber interface {
	Subscribe(topic string, handler func(msg *whisper.Message) error) (Closer, error)
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
	Request(ctx context.Context, request *whisper.Message) (resp *whisper.Message, err error)
}

type SenderCloser interface {
	Sender
	Closer
}

type Sender interface {
	Send(ctx context.Context, msg *whisper.Message) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg *whisper.Message, err error)
}
