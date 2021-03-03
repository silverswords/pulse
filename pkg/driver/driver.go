package driver

import (
	"context"
	"fmt"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/message"
)

var log = logger.NewLogger("pulse.driver")

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
func (r *pubsubRegistry) Create(name string) (Driver, error) {
	if name == "" {
		log.Info("Create default in-process driver")
	} else {
		log.Infof("Create a driver %s", name)
	}
	if method, ok := r.buses[name]; ok {
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

// if driverName is empty, use default local driver. which couldn't cross process
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

func (m *Metadata) SetDriver(driverName string) {
	m.Properties["DriverName"] = driverName
}

type Driver interface {
	Connector
	Closer
	Publisher
	Subscriber
}

// Publisher should realize the retry by themselves..
// like nats, it retry when conn is reconnecting, it would be in the pending queue.
type Publisher interface {
	Publish(message *message.Message, ctx context.Context, err error) error
}

// Subscriber is a blocking method
// should be cancel() with ctx or call Driver.Close() to close all the subscribers.
// note that handle just push the received message to subscription
type Subscriber interface {
	Subscribe(topic string, handler func(message *message.Message, ctx context.Context, err error) error) (Closer, error)
}

// Closer is the common interface for things that can be closed.
type Closer interface {
	Close() error
}

type Connector interface {
	Connect(Metadata) error
}

type DriverAsync interface {
	Driver
	// waiting for design
	PublishAsync()
	SubscribeSync()
}
