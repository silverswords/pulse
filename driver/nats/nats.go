package nats

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/message"
	"log"
	"time"
)

const (
	natsURL    = "natsURL"
	DefaultURL = "nats://39.105.141.168:4222"
)

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

func init() {
	// use to register the nats to pubsub driver factory
}

type metadata struct {
	natsURL        string
	natsOpts       []nats.Option
	queueGroupName string
}

type NatsDriver struct {
	Conn *nats.Conn
	metadata

	// one natsDriver only hold one subscriber
	closedCh chan struct{}
}

func NewNats() *NatsDriver {
	return &NatsDriver{
		closedCh: make(chan struct{}),
	}
}

// Init initializes the driver and init the connection to the server.
func (n *NatsDriver) Init(metadata whisper.Metadata) error {
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
func (n *NatsDriver) Publish(ctx context.Context, in *message.Message) error {
	err := n.Conn.Publish(in.Topic(), message.ToByte(in))
	if err != nil {
		return fmt.Errorf("nats: error from publish: %s", err)
	}
	return nil
}

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
func (n *NatsDriver) Subscribe(ctx context.Context, topic string, handler func(msg *message.Message) error)error {
	var (
		sub        *nats.Subscription
		err        error
		MsgHandler = func(m *nats.Msg) {
			msg, err := message.ToMessage(m.Data)
			if err != nil {
				//ctx.logger.Warnf("nats: error subscribe: %s", err)

				fmt.Println("Not whisper message: ", err)
				return
			}
			handler(msg)
		}
	)

	if n.metadata.queueGroupName == "" {
		sub, err = n.Conn.Subscribe(topic, MsgHandler)

	} else {
		sub, err = n.Conn.QueueSubscribe(topic, n.metadata.queueGroupName, MsgHandler)
	}

	if err != nil {
		//n.logger.Warnf("nats: error subscribe: %s", err)
		return err
	}
	//ctx.logger.Debugf("nats: subscribed to subject %s with queue group %s", sub.Subject, sub.Queue)
	select {
	case <-ctx.Done():
	case <-n.closedCh:
	}

	return sub.Drain()
}

func (n *NatsDriver) Close(ctx context.Context) error {
	n.closedCh <- struct{}{}
	return nil
}

func parseNATSMetadata(meta whisper.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[natsURL]; ok && val != "" {
		m.natsURL = val
	} else {
		return m, errors.New("nats error: missing nats URL")
	}

	if m.natsOpts == nil {
		m.natsOpts = setupConnOptions(m.natsOpts)
	}

	return m, nil
}

var _ whisper.Driver = (*NatsDriver)(nil)