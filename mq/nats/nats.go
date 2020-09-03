package nats

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/silverswords/pulse/pkg/components/mq"
)

const (
	// URL -
	URL     = "natsURL"
	Options = "natsOptions"
	//DefaultURL = "nats://39.105.141.168:4222"
	DefaultURL = "nats://nats_client:W64f8c6vG6@192.168.0.253:31476"
)

func init() {
	// use to register the nats to pubsub mq factory
	mq.Registry.Register("nats", func() mq.Driver {
		return NewNats()
	})
	//log.Println("Register the nats mq")
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}

type metadata struct {
	natsURL        string
	natsOpts       []nats.Option
	queueGroupName string
}

func parseNATSMetadata(meta mq.Metadata) (metadata, error) {
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
			return m, errors.New("nats error: missing nats Options and not use default")
		}
	} else {
		m.natsOpts = setupConnOptions(m.natsOpts)
	}

	return m, nil
}

// Driver -
type Driver struct {
	metadata
	Conn *nats.Conn
}

// NewNats -
func NewNats() *Driver {
	return &Driver{}
}

// Init initializes the mq and init the connection to the server.
func (n *Driver) Init(metadata mq.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}

	n.metadata = m
	conn, err := nats.Connect(m.natsURL, m.natsOpts...)
	if err != nil {
		return fmt.Errorf("nats: error connecting to nats at %s: %s", m.natsURL, err)
	}

	n.Conn = conn
	return nil
}

// Publish publishes a message to Nats Server with message destination topic.
func (n *Driver) Publish(topic string, in []byte) error {
	err := n.Conn.Publish(topic, in)
	if err != nil {
		return fmt.Errorf("nats: error from publish: %s", err)
	}
	return nil
}

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
// handler use to receive the message and move to top level subscriber.
func (n *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
	var (
		sub        *nats.Subscription
		err        error
		MsgHandler = func(m *nats.Msg) {
			handler(m.Data)
		}
	)

	if n.metadata.queueGroupName == "" {
		sub, err = n.Conn.Subscribe(topic, MsgHandler)

	} else {
		sub, err = n.Conn.QueueSubscribe(topic, n.metadata.queueGroupName, MsgHandler)
	}

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

// Close -
func (n *Driver) Close() error {
	n.Conn.Close()
	return nil
}

var _ mq.Driver = (*Driver)(nil)
var _ mq.Closer = (*subscriber)(nil)
