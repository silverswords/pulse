package driver

import (
	"context"
	"fmt"
	"log"
)

var Registry = pubsubRegistry{
	buses: make(map[string]func() Driver),
}

type pubsubRegistry struct {
	buses map[string]func() Driver
}

func (r *pubsubRegistry) Register(name string, factory func() Driver) {
	r.buses[name] = factory
}

// Create instantiates a pub/sub based on `name`.
func (p *pubsubRegistry) Create(name string) (Driver, error) {
	log.Println("Create a driver",name)
	if method, ok := p.buses[name]; ok {
		return method(), nil
	}
	return nil, fmt.Errorf("couldn't find message bus %s", name)
}

type Metadata struct {
	Properties map[string]interface{}
}

func NewMetadata() *Metadata {
	return &Metadata{Properties: make(map[string]interface{})}
}

// todo: default change it to local eventbus
func (m *Metadata) GetDriverName() string {
	var noDriver = ""
	if driverName, ok := m.Properties["DriverName"]; ok {
		if nameString, ok := driverName.(string); ok {
			return nameString
		}
		return noDriver
	}
	return noDriver
}
// todo: SetDriver() is need.

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
	Publish(topic string, in []byte) error
}

// Subscriber is a blocking method
// should be cancel() with ctx or call Driver.Close() to close all the subscribers.
// note that handle just push the received message to subscription
type Subscriber interface {
	Subscribe(topic string, handler func(out []byte)) (Closer, error)
}

// Closer is the common interface for things that can be closed.
type Closer interface {
	Close() error
}

type Client interface {
	Sender

	// startReceive start to handle msg := client.Receive(). with fn function.
	StartReceive(ctx context.Context, fn interface{}) error
}

type Requests interface {
	Request(ctx context.Context, request []byte) (resp []byte, err error)
}

type SenderCloser interface {
	Sender
	Closer
}

type Sender interface {
	Send(ctx context.Context, msg []byte) error
}

type Receiver interface {
	Receive(ctx context.Context) (msg []byte, err error)
}
