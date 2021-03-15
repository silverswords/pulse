package eventbus

// from https://github.com/asaskevich/EventBus/blob/master/README.md
import (
	"errors"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/pubsub"

	evb "github.com/asaskevich/EventBus"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
)

var eventbus = &Driver{eb: evb.New()}

func init() {
	// use to register the nats to pubsub driver factory
	pubsub.Registry.Register("", func() driver.Driver {
		return NewEventBus()
	})
	//log.Println("Register the nats driver")
}

type metadata struct {
}

// Driver -
type Driver struct {
	metadata
	eb      evb.Bus
	stopped bool
}

// NewEventBus -
func NewEventBus() *Driver {
	return eventbus
}

// Init initializes the driver and init the connection to the server.
func (d *Driver) Init(metadata protocol.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}
	d.metadata = m
	return nil
}

// Publish publishes a protocol to EventBus with protocol destination topic.
// note that there is no error to return.
func (d *Driver) Publish(topic string, in []byte) error {
	if d.stopped {
		return errors.New("draining")
	}
	d.eb.Publish(topic, in)
	return nil
}

// Closer -
type Closer func() error

// Close -
func (c Closer) Close() error {
	return c()
}

// Subscribe handle protocol from specific topic.
// use context to cancel the client
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a protocol and only one of the group would receive the protocol.
// handler use to receive the protocol and move to top level client.
func (d *Driver) Subscribe(topic string, handler func(msg []byte)) (driver.Closer, error) {
	if d.stopped {
		return nil, errors.New("draining")
	}
	var (
		closer Closer = func() error {
			return d.eb.Unsubscribe(topic, handler)
		}
		err error
	)

	err = d.eb.Subscribe(topic, handler)

	if err != nil {
		//n.logger.Warnf("nats: error subscribe: %s", err)
		return nil, err
	}

	return closer, nil
}

// Close -
func (d *Driver) Close() error {
	d.stopped = true
	return nil
}

func parseNATSMetadata(_ protocol.Metadata) (metadata, error) {
	m := metadata{}
	return m, nil
}

var _ driver.Driver = (*Driver)(nil)
var _ driver.Closer = (*Driver)(nil)
