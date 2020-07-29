package whisper

import (
	"context"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"
	"github.com/silverswords/whisper/internal"
	"github.com/silverswords/whisper/internal/scheduler"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	QueueCapacity = 100
)

type Subscription struct {
	subOptions []subOption

	d     driver.Driver
	topic string
	// the messages which received by the driver.
	scheduler scheduler.ReceiveScheduler

	handlers       []func(msg *Message) error
	onErr          func(error)
	// todo: combine callbackFn in handlers
	callbackFn func(msg *Message) error
	ackFn      func(msg *Message) error

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

	// DeadLetterPolicy specifies the conditions for dead lettering messages in
	// a subscription. If not set, dead lettering is disabled.
	DeadLetterPolicy *internal.DeadLetterPolicy

	// Filter is an expression written in the Cloud Pub/Sub filter language. If
	// non-empty, then only `PubsubMessage`s whose `attributes` field matches the
	// filter are delivered on this subscription. If empty, then no messages are
	// filtered out. Cannot be changed after the subscription is created.
	//
	// It is EXPERIMENTAL and a part of a closed alpha that may not be
	// accessible to all users. This field is subject to change or removal
	// without notice.
	Filter string

	// RetryPolicy specifies how Cloud Pub/Sub retries message delivery.
	RetryPolicy *internal.RetryParams

	// Detached indicates whether the subscription is detached from its topic.
	// Detached subscriptions don't receive messages from their topic and don't
	// retain any backlog. `Pull` and `StreamingPull` requests will return
	// FAILED_PRECONDITION. If the subscription is a push subscription, pushes to
	// the endpoint will not be made.
	Detached bool
	// MaxExtension is the maximum period for which the Subscription should
	// automatically extend the ack deadline for each message.
	//
	// The Subscription will automatically extend the ack deadline of all
	// fetched Messages up to the duration specified. Automatic deadline
	// extension beyond the initial receipt may be disabled by specifying a
	// duration less than 0.
	MaxExtension time.Duration

	// MaxExtensionPeriod is the maximum duration by which to extend the ack
	// deadline at a time. The ack deadline will continue to be extended by up
	// to this duration until MaxExtension is reached. Setting MaxExtensionPeriod
	// bounds the maximum amount of time before a message redelivery in the
	// event the subscriber fails to extend the deadline.
	//
	// MaxExtensionPeriod configuration can be disabled by specifying a
	// duration less than (or equal to) 0.
	MaxExtensionPeriod time.Duration

	// MaxOutstandingMessages is the maximum number of unprocessed messages
	// (unacknowledged but not yet expired). If MaxOutstandingMessages is 0, it
	// will be treated as if it were DefaultReceiveSettings.MaxOutstandingMessages.
	// If the value is negative, then there will be no limit on the number of
	// unprocessed messages.
	MaxOutstandingMessages int

	// MaxOutstandingBytes is the maximum size of unprocessed messages
	// (unacknowledged but not yet expired). If MaxOutstandingBytes is 0, it will
	// be treated as if it were DefaultReceiveSettings.MaxOutstandingBytes. If
	// the value is negative, then there will be no limit on the number of bytes
	// for unprocessed messages.
	MaxOutstandingBytes int

	// NumGoroutines is the number of goroutines that each datastructure along
	// the Receive path will spawn. Adjusting this value adjusts concurrency
	// along the receive path.
	//
	// NumGoroutines defaults to DefaultReceiveSettings.NumGoroutines.
	//
	// NumGoroutines does not limit the number of messages that can be processed
	// concurrently. Even with one goroutine, many messages might be processed at
	// once, because that goroutine may continually receive messages and invoke the
	// function passed to Receive on them. To limit the number of messages being
	//	processed concurrently, set MaxOutstandingMessages.
			NumGoroutines int

			// If Synchronous is true, then no more than MaxOutstandingMessages will be in
			// memory at one time. (In contrast, when Synchronous is false, more than
			// MaxOutstandingMessages may have been received from the service and in memory
			// before being processed.) MaxOutstandingBytes still refers to the total bytes
			// processed, rather than in memory. NumGoroutines is ignored.
			// The default is false.
			Synchronous bool
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultRecieveSettings = ReceiveSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	AckTimeout:     2 * 60 * time.Second,
	// By default, limit the bundler to 10 times the max message size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	// default linear increase retry interval and 10 times.
	RetryParams:       &internal.DefaultRetryParams,
	// default nil and drop letter.
	DeadLetterPolicy: nil,
}

// new a topic and init it with the connection options
func NewSubscription(topicName string, driverMetadata driver.Metadata, options ...subOption) (*Subscription, error) {
	driver, err := driver.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	t := &Subscription{
		topic:            topicName,
		subOptions:    options,
		d:               driver,
		pendingAcks:     make(map[string]bool),
		deadQueue:       nil,
	}

	if err := t.applyOptions(options...); err != nil {
		return nil, err
	}

	if err := t.d.Init(driverMetadata); err != nil {
		return nil, err
	}
	if err := t.startAck(); err != nil {
		t.d.Close()
		return nil, err
	}
	return t, nil
}

func (s *Subscription) done() {

}

// Receive for receive the message and return error when handle message error.
// if error, may should call DrainAck()?
func (s *Subscription) Receive(ctx context.Context, callback func(ctx context.Context, message *Message)) error {
	s.scheduler = scheduler.NewReceiveScheduler(Re),
	closer, err := s.d.Subscribe(s.topic, func(msg []byte) error {
		m, err := ToMessage(msg)
		if err != nil {
			log.Println("Error while transform the []byte to message: ", err)
		}


		return nil
	})
	if err != nil {
		return err
	}

	// start pollGoroutines worker to handle messages every function
	go func() {
		for {
			select {
			case <-s.closedCh:
				closer.Close()
				// if close only drain(), new a goroutine to consume rest of messages.
				go func() {
					for {
						if len(s.queue) == 0 {
							return
						}
						err := s.processMessage()
						if err != nil {
							s.onErr(err)
						}
					}
				}()

			default:
				// Start Polling.
				wg := sync.WaitGroup{}
				for i := 0; i < s.pollGoroutines; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							err := s.processMessage()
							if err != nil {
								s.onErr(err)
							}
						}
					}()
				}
				wg.Wait()
			}

		}
	}()

	return nil
}

func (s *Subscription) processMessage() (err error) {
	if len(s.queue) == 0 {
		return
	}
	msg := <-s.queue

	if msg.L.AckID != "" {
		s.ackFn(msg)
		//if s.ackFn == noAckFn{
		//	log.Println("need ack but not.")
		//}
	}

	for _, v := range s.handlers {
		if err = v(msg); err != nil {
			//	todo: with logger to log error
			log.Println("Error while handle message with ", v, "error: ", err)
		}
	}

	err = s.callbackFn(msg)
	if err != nil {
		log.Println("Error while handle message with callbackFn: ", s.callbackFn, "error: ", err)
	}
	return err
}

func WithMiddlewares(handlers ...func(*Message) error) subOption {
	return func(s *Subscription) error {
		s.handlers = handlers
		return nil
	}
}

func WithAck() subOption {
	return func(s *Subscription) error {
		// todo: completed the ack logic
		s.ackFn = func(m *Message) error {
			var ackMsg *Message
			acksender(s.d, ackMsg)

			return nil
		}
		return nil
	}
}

//func newAckEvent(m *Message) (ackMsg *Message) {
//	ackMsg = &Message{
//		Id: m.Id,
//		AckID: m.AckID,
//		Topic: AckTopicPrefix +m.Topic,
//	}
//	return ackMsg
//}

func acksender(d driver.Driver, m *Message) error {
	//err := d.Publish(m)
	return nil
}

func noAckFn(_ *Message) error {
	return nil
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
