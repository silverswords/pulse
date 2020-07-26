package whisper

import (
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"
	"github.com/silverswords/whisper/message"
	"runtime"
)

type Topic struct {
	topicOptions []topicOption

	d     driver.Driver
	topic string
	queue chan *message.Message
	// ackid map message
	ack             bool
	waittingMessage map[string]*message.Message

	// todo:  how to handle message with a gracefully logic.
	pollGoroutines int
	endpoints      []func(m *message.Message)
}

// new a topic and init it with the connection options
func NewTopic(topic string, driverMetadata driver.Metadata, options ...topicOption) (*Topic, error) {

	t := &Topic{topic: topic, topicOptions: options,
		queue:           make(chan *message.Message, 1),
		waittingMessage: make(map[string]*message.Message),
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

func (t *Topic) Send(m *message.Message) {
	t.queue <- m
}

// WithACK would turn on the ack function.
func WithACK() topicOption {
	return func(t *Topic) error {
		t.endpoints = append(t.endpoints, func(m *message.Message) {
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
