package eventbus

// from https://github.com/asaskevich/EventBus/blob/master/README.md
import (
	"errors"

	evb "github.com/asaskevich/EventBus"
	"github.com/silverswords/pulse/pkg/components/mq"
)

var eventbus = &Driver{eb: evb.New()}

func init() {
	// use to register the nats to pubsub mq factory
	mq.Registry.Register("", func() mq.Driver {
		return NewEventBus()
	})
	//log.Println("Register the nats mq")
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

// Init initializes the mq and init the connection to the server.
func (d *Driver) Init(metadata mq.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}
	d.metadata = m
	return nil
}

// Publish publishes a message to EventBus with message destination topic.
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

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
// handler use to receive the message and move to top level subscriber.
func (d *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
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

func parseNATSMetadata(_ mq.Metadata) (metadata, error) {
	m := metadata{}
	return m, nil
}

var _ mq.Driver = (*Driver)(nil)
var _ mq.Closer = (*Driver)(nil)
