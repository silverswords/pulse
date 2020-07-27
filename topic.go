package whisper

import (
	"context"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"
	"runtime"
	"sync"
)

type Topic struct {
	topicOptions []topicOption

	d     driver.Driver
	topic string
	queue chan *BundleMessage
	// ackid map message
	ack             bool
	waittingMessage map[string]*Message

	// todo:  how to handle message with a gracefully logic.
	pollGoroutines int
	endpoints      []func(m *Message)

	mu        sync.RWMutex
	stopped   bool
}

// new a topic and init it with the connection options
func NewTopic(topic string, driverMetadata driver.Metadata, options ...topicOption) (*Topic, error) {
	t := &Topic{
		topic: topic,
		topicOptions: options,
		queue:           make(chan *BundleMessage, QueueCapacity),
		waittingMessage: make(map[string]*Message, QueueCapacity),
		d:               nats.NewNats(),
		pollGoroutines:  runtime.GOMAXPROCS(0),
	}

	if err := t.applyOptions(options...); err != nil {
		return nil, err
	}

	t.d.Init(driverMetadata)
	t.start()
	return t, nil
}

// use to start the topic sender and acker
func (t *Topic) start() {
	t.startSender()
	if t.ack {
		t.startCycleDeadMessage()
	}
}

func (t *Topic) startSender() {
	go func() {
		for {
			m := <-t.queue
			for _, v := range t.endpoints {
				v(m)
			}
			t.d.Publish(m)
		}
	}()
}

func (t *Topic) startCycleDeadMessage() {

}

type BundleMessage struct {
	 msg *Message
	 res *PublishResult
}

type PublishResult struct {
	ready chan struct{}
	err error
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *PublishResult) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish call.
// Get blocks until the Publish call completes or the context is done.
func (r *PublishResult) Get(ctx context.Context)  (err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return  r.err
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.Ready():
		return  r.err
	}
}

func (r *PublishResult) set(sid string, err error) {
	r.serverID = sid
	r.err = err
	close(r.ready)
}

func (t *Topic) Send(m *Message) *PublishResult {
	r := &PublishResult{ready: make(chan struct{})}
	t.queue <- &BundleMessage{m,r}
	return r
}

// WithACK would turn on the ack function.
func WithACK() topicOption {
	return func(t *Topic) error {
		t.endpoints = append(t.endpoints, func(m *Message) {
			t.waittingMessage[m.AckID] = m
			//todo: change to random unique id
			m.AckID = "todo:"

		})

		// todo: sub the ack channel and do the ack delete.
		return nil
	}
}

type topicOption func(*Topic) error

func (t *Topic) applyOptions(opts ...topicOption) error {
	for _, fn := range opts {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}
