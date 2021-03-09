package subscription

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/message/protocol/retry"
	"github.com/silverswords/pulse/pkg/pubsub"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"github.com/silverswords/pulse/pkg/scheduler"
	"github.com/silverswords/pulse/pkg/topic"
	"github.com/valyala/fasthttp"
	"sync"
	"time"
)

const (
	DefaultWebHookRequestTimeout = 60 * time.Second
)

var (
	log = logger.NewLogger("pulse")

	errReceiveInProgress = errors.New("pubsub: Receive already in progress for this subscription")
)

type Subscription struct {
	subOptions []Option

	d     driver.Driver
	topic string
	// the messages which received by the driver.
	scheduler *scheduler.Scheduler

	handlers      []func(ctx context.Context, msg *message.CloudEventsEnvelope)
	webhookClient *fasthttp.Client

	// the received message so the repeated message not handle again.
	// todo: consider change it to bitmap or expired when over 60 seconds
	receivedEvent map[string]bool

	mu sync.RWMutex
	// Settings for receiving messages. All changes must be made before the
	// first call to Receive. The default is DefaultPublishSettings.
	// it means could not dynamically change and hot start.
	ReceiveSettings
	receiveActive bool
}

// could add toproto() protoToSubscriptionConfig() from https://github.com/googleapis/google-cloud-go/blob/master/pubsub/subscription.go
// SubscriptionConfig describes the configuration of a subscription.
type ReceiveSettings struct {
	// EnableMessageOrdering enables message ordering.
	//
	// It is EXPERIMENTAL and a part of a closed alpha that may not be
	// accessible to all users. This field is subject to change or removal
	// without notice.
	EnableMessageOrdering bool

	EnableAck bool

	// RetryPolicy specifies how Cloud Pub/Sub retries message delivery.
	RetryParams *retry.Params

	// MaxOutstandingMessages is the maximum number of unprocessed messages
	// (unacknowledged but not yet expired). If MaxOutstandingMessages is 0, it
	// will be treated as if it were DefaultReceiveSettings.MaxOutstandingMessages.
	// If the value is negative, then there will be no limit on the number of
	// unprocessed messages.
	MaxOutstandingMessages int

	// WebHookRequestTimeout is the timeout when Subscription calls message's callback webhook via fasthttp.Client.
	WebHookRequestTimeout time.Duration
}

// DefaultPublishSettings holds the default values for topics' Settings.
var DefaultRecieveSettings = ReceiveSettings{
	// default linear increase retry interval and 10 times.
	RetryParams: &retry.DefaultRetryParams,
	// default nil and drop letter.

	MaxOutstandingMessages: 1000,

	WebHookRequestTimeout: DefaultWebHookRequestTimeout,
}

// new a topic and init it with the connection options
func NewSubscription(topicName string, driverMetadata driver.Metadata, options ...Option) (*Subscription, error) {
	d, err := pubsub.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	s := &Subscription{
		topic:      topic.PulsePrefix + topicName,
		subOptions: options,
		d:          d,
		//handlers:        make([]func(context.Context,*Message)),
		receivedEvent:   make(map[string]bool),
		ReceiveSettings: DefaultRecieveSettings,
	}

	if err := s.applyOptions(options...); err != nil {
		return nil, err
	}

	if err := s.d.Init(driverMetadata); err != nil {
		return nil, err
	}

	return s, nil
}

// done make the message.Ack could request to send a ack event to the topic with AckTopicPrefix.
// receiveTime is not useful now because there is no required to promise to topic that suber had handled themessage.
func (s *Subscription) done(ackId string, ack bool) {
	// No ack logic
	if !ack {
		//s.pendingAcks[ackId] = ack
		return
	}
	//	send the ack event to topic and keep retry if error if connection error.
	go func() {
		var tryTimes int
		// nolint
		m, err := message.NewCloudEventsEnvelope(ackId, "subscription", "none", "ack", s.topic, "", "", []byte{})
		b, err := jsoniter.ConfigFastest.Marshal(m)
		if err != nil {
			log.Error("suber ack error", err)
		}
		err = s.d.Publish(topic.AckTopicPrefix+s.topic, b)
		//log.Println("suber ----------------------------- suber ack the",m.Id )
		for err != nil {
			// wait for sometime
			err1 := s.RetryParams.Backoff(context.TODO(), tryTimes)
			tryTimes++
			if err1 != nil {
				log.Info("error retrying send ack message id:", ackId)
			}
			err = s.d.Publish(topic.AckTopicPrefix+s.topic, b)
		}
		//s.pendingAcks[ackId] = true
		//	if reached here, the message have been send ack.
	}()
}

// checkIfReceived checkd and set true if not true previous. It returns true when subscription had received the message.
func (s *Subscription) checkIfReceived(msg *message.CloudEventsEnvelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.receivedEvent[msg.ID] {
		s.receivedEvent[msg.ID] = true
		return false
	} else {
		return true
	}
}

// todo: add batching iterator to batch every suber's message. that's need to store the messages in subscribers.
// Receive is a blocking function and return error until receive the message and occurs error when handle message.
// if error, may should call DrainAck()?
func (s *Subscription) Receive(ctx context.Context, callback func(ctx context.Context, message *message.CloudEventsEnvelope)) error {
	log.Debug("Subscription Start Receive from ", s.topic)
	s.mu.Lock()
	if s.receiveActive {
		s.mu.Unlock()
		return errReceiveInProgress
	}
	s.receiveActive = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.receiveActive = false; s.mu.Unlock() }()

	s.scheduler = scheduler.NewReceiveScheduler(s.MaxOutstandingMessages)

	// Cancel a sub-context which, when we finish a single receiver, will kick
	// off the context-aware callbacks and the goroutine below (which stops
	// all receivers, iterators, and the scheduler).
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()

	closer, err := s.d.Subscribe(s.topic, func(msg []byte) {
		e := &message.CloudEventsEnvelope{}
		err := jsoniter.Unmarshal(msg, e)
		if err != nil {
			log.Error("Error while transforming the byte to message: ", err)
			// not our pulse message. just drop it.
			return
		}
		m := &message.CloudEventsEnvelope{
			ID: e.ID,
		}
		// don't repeat the handle logic.
		if s.checkIfReceived(m) {
			log.Error("Subscriber with topic: ", s.topic, " already received this message id:", m.ID)
			return
		}
		log.Debug("Subscriber with topic: ", s.topic, " received message id: ", m.ID)

		// done is async function
		m.DoneFunc = s.done
		// if not EnableAck, Please use m.Ack() manually to ack the message.
		// promise to ack when received a right message.
		if s.EnableAck {
			// m.Ack() is async function.
			m.Ack()
		}

		// if no ordering, it would be concurrency handle the message.
		err = s.scheduler.Add(m.OrderingKey, m, func(msg interface{}) {
			// group to receive the first error and terminate all the subscribers.
			// just hint the message is not ordering handle.
			if s.EnableMessageOrdering && m.OrderingKey != "" {
				err = fmt.Errorf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", m.OrderingKey)
			}
			// handle the message until the ackTimeout is reached
			// if cannot handle out the message

			// second endpoints
			for _, v := range s.handlers {
				v(ctx2, m)
			}

			callback(ctx2, m)
		})

		if err != nil {
			cancel2()
		}
	})
	if closer != nil {
		defer closer.Close()
	}
	defer s.scheduler.Shutdown()
	// sub error
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-ctx2.Done():
		err = ctx2.Err()
	}

	// if there is some error. close the suber and return the error.
	return err
}
