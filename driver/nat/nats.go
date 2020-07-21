package nat

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
	natsURL = "natsURL"
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
	natsURL string
	natsOpts []nats.Option
}
type NatsDriver struct {
	Conn *nats.Conn
	metadata

	// todo: add a logger module
	logger interface{}
}

func NewNats() *NatsDriver {
	return &NatsDriver{}
}

func (n *NatsDriver) Init(metadata whisper.Metadata) error {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}

	n.metadata = m
	conn, err := nats.Connect(m.natsURL,m.natsOpts...)
	if err != nil {
		return fmt.Errorf("nats: error connecting to nats at %s: %s", m.natsURL, err)
	}

	n.Conn = conn
	return nil
}

func (n *NatsDriver) Publish(ctx context.Context, in *message.Message) error {
	err := n.Conn.Publish(req.Topic, req.Data)
	if err != nil {
		return fmt.Errorf("nats: error from publish: %s", err)
	}
	return nil
}

func (n *NatsDriver) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	sub, err := n.Conn.QueueSubscribe(req.Topic, n.metadata.natsQueueGroupName, func(natsMsg *nats.Msg) {
		handler(&pubsub.NewMessage{Topic: req.Topic, Data: natsMsg.Data})
	})
	if err != nil {
		n.logger.Warnf("nats: error subscribe: %s", err)
	}
	n.logger.Debugf("nats: subscribed to subject %s with queue group %s", sub.Subject, sub.Queue)

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