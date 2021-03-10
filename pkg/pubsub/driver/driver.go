package driver

import (
	"context"
	"github.com/silverswords/pulse/pkg/message"
)

// If a Driver implements DriverContext, then sql.DB will call
// OpenConnector to obtain a Connector and then invoke
// that Connector's Connect method to obtain each needed connection,
// instead of invoking the Driver's Open method for each connection.
// The two-step sequence allows drivers to parse the name just once
// and also provides access to per-Conn contexts.
type DriverContext interface {
	OpenConnector(Metadata) (Connector, error)
}

type Driver interface {
	Open(Metadata) (Conn, error)
}

type Conn interface {
	Publisher
	Subscriber
	Closer
}

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to sql.OpenDB, to allow drivers
// to implement their own sql.DB constructors, or returned by
// DriverContext's OpenConnector method, to allow drivers
// access to context and to avoid repeated parsing of driver
// configuration.
type Connector interface {
	// Connect returns a connection to the database.
	// Connect may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The provided context.Context is for dialing purposes only
	// (see net.DialContext) and should not be stored or used for
	// other purposes. A default timeout should still be used
	// when dialing as a connection pool may call Connect
	// asynchronously to any query.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Connect(context.Context) (Conn, error)

	// Driver returns the underlying Driver of the Connector,
	// mainly to maintain compatibility with the Driver method
	// on sql.DB.
	Driver() Driver
}

// Publisher should realize the retry by themselves..
// like nats, it retry when conn is reconnecting, it would be in the pending queue.
type Publisher interface {
	Publish(r *PublishRequest, ctx context.Context, err error) error
}

// Subscriber is a blocking method
// should be cancel() with ctx or call Driver.Close() to close all the subscribers.
// note that handle just push the received message to subscription
type Subscriber interface {
	Subscribe(ctx context.Context, r *SubscribeRequest, handler func(message *message.Message, ctx context.Context, err error) error) (Subscription, error)
}

type Subscription interface {
	Closer
}

// Closer is the common interface for things that can be closed.
type Closer interface {
	Close() error
}

type ConnAsync interface {
	// waiting for design
	PublishAsync()
	SubscribeSync()
}

// PublishRequest is the request to publish a message
type PublishRequest struct {
	Message    message.Message `json:"data"`
	PubsubName string          `json:"pubsubname"`
	Topic      string          `json:"topic"`
	Metadata   Metadata        `json:"metadata"`
}

// SubscribeRequest is the request to subscribe to a topic
type SubscribeRequest struct {
	Topic    string   `json:"topic"`
	Metadata Metadata `json:"metadata"`
}

type Metadata struct {
	Properties map[string]string
}

func NewMetadata() *Metadata {
	return &Metadata{Properties: make(map[string]string)}
}

func (m *Metadata) Clone() *Metadata {
	newProperties := make(map[string]string)
	for k, v := range m.Properties {
		newProperties[k] = v
	}
	return &Metadata{Properties: newProperties}
}

// if driverName is empty, use default local driver. which couldn't cross process
func (m *Metadata) GetDriverName() string {
	var noDriver = ""
	if driverName, ok := m.Properties["DriverName"]; ok {
		return driverName
	}
	return noDriver
}

func (m *Metadata) SetDriver(driverName string) {
	m.Properties["DriverName"] = driverName
}
