package eventbus

// from https://github.com/asaskevich/EventBus/blob/master/README.md
import (
	"errors"
	evb "github.com/asaskevich/EventBus"
	"github.com/nats-io/nats.go"
	"github.com/silverswords/whisper/driver"
)

const ()

func init() {
	// use to register the nats to pubsub driver factory
	driver.Registry.Register("", func() driver.Driver {
		return NewEventBus()
	})
	//log.Println("Register the nats driver")
}

type metadata struct {
	natsURL        string
	natsOpts       []nats.Option
	queueGroupName string
}

type Driver struct {
	metadata
	eb evb.Bus
}

func NewEventBus() *Driver {
	return &Driver{}
}

// Init initializes the driver and init the connection to the server.
func (d *Driver) Init(metadata driver.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}

	d.metadata = m
	eb := evb.New()
	d.eb = eb
	return nil
}

// Publish publishes a message to EventBus with message destination topic.
// note that there is no error to return.
func (d *Driver) Publish(topic string, in []byte) error {
	d.eb.Publish(topic, in)
	return nil
}

type SuberCloser func() error

func (c SuberCloser) Close() error {
	return c()
}

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
// handler use to receive the message and move to top level subscriber.
func (d *Driver) Subscribe(topic string, handler func(msg []byte)) (driver.Closer, error) {
	var (
		sub SuberCloser = func() {

		}
		err        error
		MsgHandler = func(m *nats.Msg) {
			handler(m.Data)
		}
	)

	err = d.eb.Subscribe(topic, handler)
	if err != nil {
		//n.logger.Warnf("nats: error subscribe: %s", err)
		return nil, err
	}

	return &subscriber{sub: sub}, nil
}

type subscriber struct {
	sub *nats.Subscription
}

// Close subscriber to unsubscribe topic but not close connection.
func (s *subscriber) Close() error {
	return s.sub.Drain()
}

func (n *NatsDriver) Close() error {
	n.Conn.Close()
	return nil
}

func parseNATSMetadata(meta driver.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[URL]; ok && val != "" {
		if m.natsURL, ok = val.(string); !ok {
			return m, errors.New("nats error: nats URL is not a string")
		}
	} else {
		return m, errors.New("nats error: missing nats URL")
	}

	if val, ok := meta.Properties[Options]; ok && val != nil {
		if m.natsOpts, ok = val.([]nats.Option); !ok {
			return m, errors.New("nats error: missing nats Options and not use default.")
		}
	} else {
		m.natsOpts = setupConnOptions(m.natsOpts)
	}

	return m, nil
}

var _ driver.Driver = (*NatsDriver)(nil)
var _ driver.Closer = (*subscriber)(nil)
