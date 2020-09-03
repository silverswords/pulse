package subscription

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/pulse/pkg/components/mq"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/retry"
	"github.com/silverswords/pulse/pkg/scheduler"
	"github.com/silverswords/pulse/pkg/topic"
	"sync"
	"sync/atomic"
)

var (
	log = logger.NewLogger("pulse")

	errReceiveInProgress = errors.New("pubsub: Receive already in progress for this subscription")
)

type Subscription struct {
	subOptions []Option

	d     mq.Driver
	topic string
	// the messages which received by the mq.
	scheduler *scheduler.ReceiveScheduler

	handlers []func(ctx context.Context, msg *message.Message)

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
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultRecieveSettings = ReceiveSettings{
	// default linear increase retry interval and 10 times.
	RetryParams: &retry.DefaultRetryParams,
	// default nil and drop letter.

	MaxOutstandingMessages: 1000,
}

// new a topic and init it with the connection options
func NewSubscription(topicName string, driverMetadata mq.Metadata, options ...Option) (*Subscription, error) {
	d, err := mq.Registry.Create(driverMetadata.GetDriverName())
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
		m := &message.Message{Id: ackId}
		err := s.d.Publish(topic.AckTopicPrefix+s.topic, message.ToByte(m))
		//log.Println("suber ----------------------------- suber ack the",m.Id )
		for err != nil {
			// wait for sometime
			err1 := s.RetryParams.Backoff(context.TODO(), tryTimes)
			tryTimes++
			if err1 != nil {
				log.Info("error retrying send ack message id:", m.Id)
			}
			err = s.d.Publish(topic.AckTopicPrefix+s.topic, message.ToByte(m))
		}
		//s.pendingAcks[ackId] = true
		//	if reached here, the message have been send ack.
	}()
}

// checkIfReceived checkd and set true if not true previous. It returns true when subscription had received the message.
func (s *Subscription) checkIfReceived(msg *message.Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.receivedEvent[msg.Id] {
		s.receivedEvent[msg.Id] = true
		return false
	} else {
		return true
	}
}

// todo: add batching iterator to batch every suber's message. that's need to store the messages in subscribers.
// Receive is a blocking function and return error until receive the message and occurs error when handle message.
// if error, may should call DrainAck()?
func (s *Subscription) Receive(ctx context.Context, callback func(ctx context.Context, message *message.Message)) error {
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
		m, err := message.ToMessage(msg)
		if err != nil {
			log.Error("Error while transforming the byte to message: ", err)
			// not our pulse message. just drop it.
			return
		}
		// don't repeat the handle logic.
		if s.checkIfReceived(m) {
			log.Error("Subscriber with topic: ", s.topic, " already received this message id:", m.Id)
			return
		}
		log.Debug("Subscriber with topic: ", s.topic, " received message id: ", m.Id)

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
	return ctx2.Err()
}

func WithMiddlewares(handlers ...func(context.Context, *message.Message)) Option {
	return func(s *Subscription) error {
		s.handlers = append(s.handlers, handlers...)
		return nil
	}
}

func WithCount() Option {
	return func(s *Subscription) error {
		var count uint64
		s.handlers = append(s.handlers, func(ctx context.Context, m *message.Message) {
			atomic.AddUint64(&count, 1)
			log.Info("count: ", count)
		})
		return nil
	}
}

func WithAutoACK() Option {
	return func(s *Subscription) error {
		s.EnableAck = true
		return nil
	}
}

type Option func(*Subscription) error

func (s *Subscription) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}
