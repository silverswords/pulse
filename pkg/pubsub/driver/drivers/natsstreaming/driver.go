package natsstreaming

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/stan.go/pb"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/pubsub"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
)

// Nats url format with user credentials:
// DefaultURL = "nats://nats_client:W64f8c6vG6@192.168.0.253:31476"

// compulsory options
const (
	natsURL                = "natsURL"
	natsStreamingClusterID = "natsStreamingClusterID"
)

// subscription options (optional)
const (
	durableSubscriptionName = "durableSubscriptionName"
	startAtSequence         = "startAtSequence"
	startWithLastReceived   = "startWithLastReceived"
	deliverAll              = "deliverAll"
	deliverNew              = "deliverNew"
	startAtTimeDelta        = "startAtTimeDelta"
	startAtTime             = "startAtTime"
	startAtTimeFormat       = "startAtTimeFormat"
	ackWaitTime             = "ackWaitTime"
	maxInFlight             = "maxInFlight"
)

// valid values for subscription options
const (
	subscriptionTypeQueueGroup = "queue"
	subscriptionTypeTopic      = "topic"
	startWithLastReceivedTrue  = "true"
	deliverAllTrue             = "true"
	deliverNewTrue             = "true"
)

const (
	consumerID       = "consumerID" // passed in by Dapr runtime
	subscriptionType = "subscriptionType"
)

func init() {
	// use to register the nats to pubsub driver factory
	pubsub.Registry.Register("nats", func(logger logger.Logger) driver.Driver {
		return NewNatsStreamingDriver(logger)
	})
	log.Println("Register the nats streaming driver")
}

func NewConnector(metadata protocol.Metadata, logger logger.Logger) (driver.Connector, error) {
	m, err := parseNATSStreamingMetadata(metadata)
	if err != nil {
		return nil, err
	}
	c := &connector{metadata: m, logger: logger}
	return c, nil
}

type metadata struct {
	natsURL                 string
	natsStreamingClusterID  string
	subscriptionType        string
	natsQueueGroupName      string
	durableSubscriptionName string
	startAtSequence         uint64
	startWithLastReceived   string
	deliverNew              string
	deliverAll              string
	startAtTimeDelta        time.Duration
	startAtTime             string
	startAtTimeFormat       string
	ackWaitTime             time.Duration
	maxInFlight             uint64
}

// NatsStreamingPubSubDriver -
type PubSubDriver struct {
	log logger.Logger
}

// Todo: natsstreaming realized ack, queue sub but not ordering.
// Features design from dapr components-contrib.
func (d *PubSubDriver) Features() map[string]bool {
	return map[string]bool{}
}

func (d *PubSubDriver) SatisfyFeatures(m protocol.Metadata) ([]string, bool) {
	featuresMap := d.Features()
	notSupported := make([]string, 0)
	for i := range m.Properties {
		if !featuresMap[i] {
			notSupported = append(notSupported, i)
		}
	}
	if len(notSupported) != 0 {
		return notSupported, false
	}
	return notSupported, true
}

func (d *PubSubDriver) OpenConnector(m protocol.Metadata) (driver.Connector, error) {
	if features, ok := d.SatisfyFeatures(m); !ok {
		return nil, errors.New(fmt.Sprint("no support for these features: ", features))
	}
	return NewConnector(m, d.log)
}

// NewNatsStreamingDriver returns a new NATS Streaming pub-sub implementation
func NewNatsStreamingDriver(logger logger.Logger) driver.Driver {
	return &PubSubDriver{log: logger}
}

// Connect initializes the driver and init the connection to the server.
func (d *PubSubDriver) Open(metadata protocol.Metadata) (driver.Conn, error) {
	c, err := NewConnector(metadata, d.log)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

type natsStreamingConn struct {
	stanConn stan.Conn

	logger logger.Logger
	//	otherParameters
	timeout time.Duration
}

// Subscribe handle protocol from specific topic.
// use context to cancel the subscription
// in metadata:
// - queueGroupName if not "", will have a queueGroup to receive a protocol and only one of the group would receive the protocol.
// handler use to receive the protocol and move to top level subscription.
func (c *natsStreamingConn) Subscribe(ctx context.Context, r *protocol.SubscribeRequest, handler func(ctx context.Context, r interface{}) error) (driver.Subscription, error) {
	var (
		sub        stan.Subscription
		err        error
		MsgHandler = func(m *stan.Msg) {
			err = handler(ctx, &protocol.Message{Topic: r.Topic, Data: m.Data})
			if err == nil {
				// todo: use custom protocol.Message.ack()
				_ = m.Ack()
			}
		}
	)
	m, err := parseNATSStreamingMetadata(r.Metadata)
	if err != nil {
		return nil, err
	}
	opts, err := m.subscriptionOptions()
	if err != nil {
		return nil, fmt.Errorf("nats-streaming: error getting subscription options %s", err)
	}

	if m.subscriptionType == subscriptionTypeTopic {
		sub, err = c.stanConn.Subscribe(r.Topic, MsgHandler, opts...)
	} else {
		sub, err = c.stanConn.QueueSubscribe(r.Topic, m.natsQueueGroupName, MsgHandler, opts...)
	}

	if err != nil {
		//n.logger.Warnf("nats: error subscribe: %s", err)
		return nil, err
	}
	if m.subscriptionType == subscriptionTypeTopic {
		c.logger.Debugf("nats: subscribed to subject %s", r.Topic)
	} else if m.subscriptionType == subscriptionTypeQueueGroup {
		c.logger.Debugf("nats: subscribed to subject %s with queue group %s", r.Topic, m.natsQueueGroupName)
	}
	return &subscription{sub: sub}, nil
}

func (c *natsStreamingConn) Close() error {
	return c.stanConn.Close()
}

// Publish publishes a protocol to Nats Server with protocol destination topic.
func (c *natsStreamingConn) Publish(ctx context.Context, r *protocol.PublishRequest) error {
	select {
	case <-ctx.Done():
		return errors.New("context cancelled")
	default:
	}
	errCh := make(chan error)
	go func() {
		err := c.stanConn.Publish(r.Topic, r.Message.Data)
		if err != nil {
			errCh <- fmt.Errorf("nats: error from publish: %s", err)
		}
		errCh <- nil
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return errors.New("context cancelled")
	}
}

type subscription struct {
	sub stan.Subscription
}

// Close subscription to unsubscribe topic but not close connection.
func (s *subscription) Close() error {
	return s.sub.Close()
}

func parseNATSStreamingMetadata(meta protocol.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[natsURL]; ok && val != "" {
		m.natsURL = val
	} else {
		return m, errors.New("nats-streaming error: missing nats URL")
	}
	if val, ok := meta.Properties[natsStreamingClusterID]; ok && val != "" {
		m.natsStreamingClusterID = val
	} else {
		return m, errors.New("nats-streaming error: missing nats streaming cluster ID")
	}

	if val, ok := meta.Properties[subscriptionType]; ok {
		if val == subscriptionTypeTopic || val == subscriptionTypeQueueGroup {
			m.subscriptionType = val
		} else {
			return m, errors.New("nats-streaming error: valid value for subscriptionType is topic or queue")
		}
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.natsQueueGroupName = val
	} else {
		return m, errors.New("nats-streaming error: missing queue group name")
	}

	if val, ok := meta.Properties[durableSubscriptionName]; ok && val != "" {
		m.durableSubscriptionName = val
	}

	if val, ok := meta.Properties[ackWaitTime]; ok && val != "" {
		dur, err := time.ParseDuration(meta.Properties[ackWaitTime])
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		m.ackWaitTime = dur
	}
	if val, ok := meta.Properties[maxInFlight]; ok && val != "" {
		max, err := strconv.ParseUint(meta.Properties[maxInFlight], 10, 64)
		if err != nil {
			return m, fmt.Errorf("nats-streaming error in parsemetadata for maxInFlight: %s ", err)
		}
		if max < 1 {
			return m, errors.New("nats-streaming error: maxInFlight should be equal to or more than 1")
		}
		m.maxInFlight = max
	}

	//nolint:nestif
	// subscription options - only one can be used
	if val, ok := meta.Properties[startAtSequence]; ok && val != "" {
		// nats streaming accepts a uint64 as sequence
		seq, err := strconv.ParseUint(meta.Properties[startAtSequence], 10, 64)
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		if seq < 1 {
			return m, errors.New("nats-streaming error: startAtSequence should be equal to or more than 1")
		}
		m.startAtSequence = seq
	} else if val, ok := meta.Properties[startWithLastReceived]; ok {
		// only valid value is true
		if val == startWithLastReceivedTrue {
			m.startWithLastReceived = val
		} else {
			return m, errors.New("nats-streaming error: valid value for startWithLastReceived is true")
		}
	} else if val, ok := meta.Properties[deliverAll]; ok {
		// only valid value is true
		if val == deliverAllTrue {
			m.deliverAll = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverAll is true")
		}
	} else if val, ok := meta.Properties[deliverNew]; ok {
		// only valid value is true
		if val == deliverNewTrue {
			m.deliverNew = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverNew is true")
		}
	} else if val, ok := meta.Properties[startAtTimeDelta]; ok && val != "" {
		dur, err := time.ParseDuration(meta.Properties[startAtTimeDelta])
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		m.startAtTimeDelta = dur
	} else if val, ok := meta.Properties[startAtTime]; ok && val != "" {
		m.startAtTime = val
		if val, ok := meta.Properties[startAtTimeFormat]; ok && val != "" {
			m.startAtTimeFormat = val
		} else {
			return m, errors.New("nats-streaming error: missing value for startAtTimeFormat")
		}
	}

	return m, nil
}

func (metadata *metadata) subscriptionOptions() ([]stan.SubscriptionOption, error) {
	var options []stan.SubscriptionOption

	if metadata.durableSubscriptionName != "" {
		options = append(options, stan.DurableName(metadata.durableSubscriptionName))
	}

	switch {
	case metadata.deliverNew == deliverNewTrue:
		options = append(options, stan.StartAt(pb.StartPosition_NewOnly))
	case metadata.startAtSequence >= 1: // messages index start from 1, this is a valid check
		options = append(options, stan.StartAtSequence(metadata.startAtSequence))
	case metadata.startWithLastReceived == startWithLastReceivedTrue:
		options = append(options, stan.StartWithLastReceived())
	case metadata.deliverAll == deliverAllTrue:
		options = append(options, stan.DeliverAllAvailable())
	case metadata.startAtTimeDelta > (1 * time.Nanosecond): // as long as its a valid time.Duration
		options = append(options, stan.StartAtTimeDelta(metadata.startAtTimeDelta))
	case metadata.startAtTime != "":
		if metadata.startAtTimeFormat != "" {
			startTime, err := time.Parse(metadata.startAtTimeFormat, metadata.startAtTime)
			if err != nil {
				return nil, err
			}
			options = append(options, stan.StartAtTime(startTime))
		}
	}

	// default is auto ACK. switching to manual ACK since processing errors need to be handled
	options = append(options, stan.SetManualAckMode())

	// check if set the ack options.
	if metadata.ackWaitTime > (1 * time.Nanosecond) {
		options = append(options, stan.AckWait(metadata.ackWaitTime))
	}
	if metadata.maxInFlight >= 1 {
		options = append(options, stan.MaxInflight(int(metadata.maxInFlight)))
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

var _ driver.Driver = (*PubSubDriver)(nil)
var _ driver.DriverContext = (*PubSubDriver)(nil)
var _ driver.Conn = (*natsStreamingConn)(nil)
var _ driver.Closer = (*subscription)(nil)
