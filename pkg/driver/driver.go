package driver

import (
	"context"
	"fmt"
	"github.com/silverswords/pulse/pkg/logger"
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
	OpenPublisher(*Metadata) Publisher
	OpenSubscriber(*Metadata) Subscriber
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

type PublisherContext interface {
	Publish(ctx context.Context, topic string, in []byte) error
}

// Closer is the common interface for things that can be closed.
type Closer interface {
	Close() error
}

type DriverContext interface {
	OpenConnector(name string) (Connector, error)
}

type Initer interface {
	Init(Metadata) error
}

type Connector interface {
	Connect(ctx context.Context) (string, error)
	Driver() Driver
}
