package whisper

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/internal"
	"github.com/silverswords/whisper/internal/scheduler"
	"log"
	"sync"
	"time"
)

var (
	errReceiveInProgress = errors.New("pubsub: Receive already in progress for this subscription")
)

type Subscription struct {
	subOptions []subOption

	d     driver.Driver
	topic string
	// the messages which received by the driver.
	scheduler *scheduler.ReceiveScheduler

	handlers []func(ctx context.Context, msg *Message)

	// the received message so the repeated message not handle again.
	// todo: consider change it to bitmap
	receivedEvent map[string]bool

	pendingAcks map[string]bool
	deadQueue   chan *Message

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

	// DeadLetterPolicy specifies the conditions for dead lettering messages in
	// a subscription. If not set, dead lettering is disabled.
	DeadLetterPolicy *internal.DeadLetterPolicy

	// RetryPolicy specifies how Cloud Pub/Sub retries message delivery.
	RetryParams *internal.RetryParams

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
	RetryParams: &internal.DefaultRetryParams,
	// default nil and drop letter.
	DeadLetterPolicy: nil,

	MaxOutstandingMessages: 1000,
}

// new a topic and init it with the connection options
func NewSubscription(topicName string, driverMetadata driver.Metadata, options ...subOption) (*Subscription, error) {
	d, err := driver.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	t := &Subscription{
		topic:           topicName,
		subOptions:      options,
		d:               d,
		receivedEvent:   make(map[string]bool),
		pendingAcks:     make(map[string]bool),
		ReceiveSettings: DefaultRecieveSettings,
	}

	if err := t.applyOptions(options...); err != nil {
		return nil, err
	}

	if err := t.d.Init(driverMetadata); err != nil {
		return nil, err
	}

	return t, nil
}

// done make the message.Ack could request to send a ack event to the topic with AckTopicPrefix.
// receiveTime is not useful now because there is no required to promise to topic that suber had handled themessage.
func (s *Subscription) done(ackId string, ack bool, receiveTime time.Time) {
	// No ack logic
	if !ack {
		s.pendingAcks[ackId] = true
		return
	}
	//	send the ack event to topic and keep retry if error if connection error.
	go func() {
		var tryTimes int
		m := &Message{L: Logic{AckID: ackId, DeliveryAttempt: &tryTimes}}
		err := s.d.Publish(AckTopicPrefix+s.topic, ToByte(m))

		for err != nil {
			// wait for sometime
			err1 := s.RetryParams.Backoff(context.TODO(), *m.L.DeliveryAttempt)
			if err1 != nil {
				//	retry and then handle the deadletter with s.receiveSettings.dead_letter_policy
				if s.DeadLetterPolicy == nil {
					return
				}
			}
			err = s.d.Publish(AckTopicPrefix+s.topic, ToByte(m))
		}
		//	if reached here, the message have been send ack.
	}()
}

// checkIfReceived checkd and set true if not true previous. It returns true when subscription had received the message.
func (s *Subscription) checkIfReceived(msg *Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.receivedEvent[msg.Id] {
		s.receivedEvent[msg.Id] = true
		return false
	} else {
		return true
	}
}

// Receive for receive the message and return error when handle message error.
// if error, may should call DrainAck()?
func (s *Subscription) Receive(ctx context.Context, callback func(ctx context.Context, message *Message)) error {
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
		m, err := ToMessage(msg)
		if err != nil {
			log.Println("Error while transform the []byte to message: ", err)
		}
		// don't repeat the handle logic.
		if s.checkIfReceived(m) {
			return
		}

		m.L.doneFunc = s.done
		// promise to ack when received a right message.
		if s.EnableAck {
			defer m.Ack()
		} else {
			defer m.Nack()
		}

		// if no ordering, it would be concurrency handle the message.
		err = s.scheduler.Add(m.L.OrderingKey, m, func(msg interface{}) {
			// group to receive the first error and terminate all the subscribers.
			// just hint the message is not ordering handle.
			if s.EnableMessageOrdering && m.L.OrderingKey != "" {
				err = fmt.Errorf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", m.L.OrderingKey)
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

	_ = closer.Close()
	s.scheduler.Shutdown()

	// if there is some error. close the suber and return the error.
	return ctx2.Err()
}

func WithMiddlewares(handlers ...func(context.Context, *Message)) subOption {
	return func(s *Subscription) error {
		s.handlers = handlers
		return nil
	}
}

func WithAck() subOption {
	return func(s *Subscription) error {
		s.EnableAck = true
		return nil
	}
}

type subOption func(*Subscription) error

func (s *Subscription) applyOptions(opts ...subOption) error {
	for _, fn := range opts {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}
