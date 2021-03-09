package natsstreaming

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/stan.go/pb"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
)

const (
	// URL -
	URL     = "natsURL"
	Options = "natsOptions"
	//DefaultURL = "nats://39.105.141.168:4222"
	DefaultURL = "nats://nats_client:W64f8c6vG6@192.168.0.253:31476"
)

func init() {
	// use to register the nats to pubsub driver factory
	pubsub.Registry.Register("nats", func() driver.Driver {
		return NewNatsStreamingDriver()
	})
	//log.Println("Register the nats driver")
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
	natsURL                string
	natsStreamingClusterID string
	natsOpts               []nats.Option
	queueGroupName         string
}

func parseNATSMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	//nolint:nestif
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
	name string `json:"name"`
	metadata
	Conn stan.Conn

	logger logger.Logger

	// design from dapr pubsub package
	ctx    context.Context
	cancel context.CancelFunc
}

type Conn struct {
	conn stan.Conn
}

func (d *Driver) Name() string {
	return d.name
}

// NewNatsStreamingDriver returns a new NATS Streaming pub-sub implementation
func NewNatsStreamingDriver(logger logger.Logger) driver.Driver {
	return &Driver{logger: logger}
}

// Connect initializes the driver and init the connection to the server.
func (d *Driver) Connect(metadata pubsub.Metadata) (Conn, error) {
	m, err := parseNATSMetadata(metadata)
	if err != nil {
		return nil
	}

	clientID := genRandomString(20)
	d.metadata = m
	m.natsOpts = append(m.natsOpts, nats.Name(clientID))
	natsConn, err := nats.Connect(m.natsURL, m.natsOpts...)
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", m.natsURL, err)
	}
	natStreamingConn, err := stan.Connect(m.natsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", m.natsStreamingClusterID, err)
	}
	d.logger.Debugf("connected to natsstreaming at %s", m.natsURL)

	ctx, cancel := context.WithCancel(context.Background())
	d.ctx = ctx
	d.cancel = cancel

	d.Conn = natStreamingConn
	return nil
}

// Publish publishes a message to Nats Server with message destination topic.
func (d *Driver) Publish(message *message.Message, ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	errCh := make(chan error)
	go func() {
		err = d.Conn.Publish(message.Topic, message.Data)
		if err != nil {
			errCh <- fmt.Errorf("nats: error from publish: %s", err)
		}
		errCh <- nil
	}()
	select {
	case err = <-errCh:
		return err
	case <-ctx.Done():
		return errors.New("context cancelled")
	}
}

// Subscribe handle message from specific topic.
// use context to cancel the subscriber
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a message and only one of the group would receive the message.
// handler use to receive the message and move to top level subscriber.
func (d *Driver) Subscribe(topic string, handler func(message *message.Message, ctx context.Context, err error) error) (driver.Closer, error) {
	var (
		sub        *nats.Subscription
		err        error
		MsgHandler = func(m *stan.Msg) {
			err = handler(&message.Message{Topic: topic, Data: m.Data}, context.Background(), err)
			if err != nil {
			}
			m.Ack()
		}
	)

	subOptions, err := d.subscriptionOptions()
	if err != nil {
		return nil, fmt.Errorf("nats-streaming: error getting subscription options %s", err)
	}

	if d.metadata.queueGroupName == "" {
		sub, err = d.Conn.Subscribe(topic, MsgHandler)
	} else {
		sub, err = d.Conn.QueueSubscribe(topic, d.metadata.queueGroupName, MsgHandler)
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

func (d *Driver) subscriptionOptions() ([]stan.SubscriptionOption, error) {
	var options []stan.SubscriptionOption

	if d.metadata.durableSubscriptionName != "" {
		options = append(options, stan.DurableName(d.metadata.durableSubscriptionName))
	}

	switch {
	case d.metadata.deliverNew == deliverNewTrue:
		options = append(options, stan.StartAt(pb.StartPosition_NewOnly))
	case d.metadata.startAtSequence >= 1: // messages index start from 1, this is a valid check
		options = append(options, stan.StartAtSequence(d.metadata.startAtSequence))
	case d.metadata.startWithLastReceived == startWithLastReceivedTrue:
		options = append(options, stan.StartWithLastReceived())
	case d.metadata.deliverAll == deliverAllTrue:
		options = append(options, stan.DeliverAllAvailable())
	case d.metadata.startAtTimeDelta > (1 * time.Nanosecond): // as long as its a valid time.Duration
		options = append(options, stan.StartAtTimeDelta(d.metadata.startAtTimeDelta))
	case d.metadata.startAtTime != "":
		if d.metadata.startAtTimeFormat != "" {
			startTime, err := time.Parse(d.metadata.startAtTimeFormat, d.metadata.startAtTime)
			if err != nil {
				return nil, err
			}
			options = append(options, stan.StartAtTime(startTime))
		}
	}

	// default is auto ACK. switching to manual ACK since processing errors need to be handled
	options = append(options, stan.SetManualAckMode())

	// check if set the ack options.
	if d.metadata.ackWaitTime > (1 * time.Nanosecond) {
		options = append(options, stan.AckWait(d.metadata.ackWaitTime))
	}
	if d.metadata.maxInFlight >= 1 {
		options = append(options, stan.MaxInflight(int(d.metadata.maxInFlight)))
	}

	return options, nil
}

const inputs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

// generates a random string of length 20
func genRandomString(n int) string {
	b := make([]byte, n)
	s := rand.NewSource(int64(time.Now().Nanosecond()))
	for i := range b {
		b[i] = inputs[s.Int63()%int64(len(inputs))]
	}
	clientID := string(b)

	return clientID
}

// Todo: natsstreaming realized ack, queue sub but not ordering.
// Features design from dapr components-contrib.
func (d *Driver) Features() []string {
	return nil
}

// Close -
func (d *Driver) Close() error {
	d.Conn.Close()
	return nil
}

var _ driver.Driver = (*Driver)(nil)
var _ driver.Closer = (*subscriber)(nil)
