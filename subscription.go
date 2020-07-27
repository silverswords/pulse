package whisper

import (
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"
	"log"
	"runtime"
	"sync"
)

const (
	QueueCapacity = 100
	AckTopicPrefix = "_ack_"
)

type Subscription struct {
	subOptions []subOption

	d     driver.Driver
	topic string
	// the messages which received by the driver.
	queue chan *Message
	// todo: consider that if need use mutex because of the elements in the queue is pointer. maybe some middleware change the message's attributes like retrytime and ack bool, would be in wrong logic.
	//messageMutex sync.Mutex

	pollGoroutines int
	handlers       []func(msg *Message) error
	onErr          func(error)
	// todo: combine callbackFn in handlers
	callbackFn func(msg *Message) error
	ackFn      func(msg *Message) error

	closedCh chan struct{}
}

// NewSubscription return a Subscription which handle the messages received by the driver.
// default no AckFn and when open message should be with its ackid is not ""
func NewSubscription(topic string, driverMetadata driver.Metadata, options ...subOption) (*Subscription, error) {
	s := &Subscription{
		topic: topic,
		subOptions: options,
		queue:          make(chan *Message, 100),
		d:              nats.NewNats(),
		pollGoroutines: runtime.GOMAXPROCS(0),
		ackFn:          noAckFn,
		onErr: func(err error) { return},
		closedCh:make(chan struct{}),
	}

	if err := s.applyOptions(options...); err != nil {
		return nil, err
	}

	s.d.Init(driverMetadata)
	s.startReceive()
	return s, nil
}

func (s *Subscription) Close() error{
	s.closedCh <- struct{}{}
	return nil
}

//
func (s *Subscription) startReceive() error {
	closer, err := s.d.Subscribe(s.topic, func(msg *Message) error {
		s.queue <- msg
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

	if msg.AckID != "" {
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

func WithMiddlewares(handlers ...func(*Message)error ) subOption{
	return func(s *Subscription) error {
		s.handlers =  handlers
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

func newAckEvent(m *Message) (ackMsg *Message) {
	ackMsg = &Message{
		Id: m.Id,
		AckID: m.AckID,
		Topic: AckTopicPrefix +m.Topic,
	}
	return ackMsg
}

func acksender(d driver.Driver, m *Message) error {
	err := d.Publish(m)
	return err
}

func noAckFn(_ *Message) error {
	return nil
}


type subOption func(*Subscription) error

func (c *Subscription) applyOptions(opts ...subOption) error {
	for _, fn := range opts {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}
