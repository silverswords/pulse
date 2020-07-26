package whisper

import (
	"context"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/message"
)

type Topic struct {
	topicOptions []topicOption

	d     driver.Driver
	topic string
	queue chan *message.Message
	// ackid map message
	waittingMessage map[string]struct {
		*message.Message
		ack bool
	}

	timeout time.Duration
}

func NewTopic(topic string, options ...topicOption) (*Topic, error) {
	t := &Topic{topic: topic, topicOptions: options}

	if err := t.applyOptions(options...); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Topic) startSender() {
	go func() {
		for {
			m := <-t.queue
			t.d.Publish(ctx, m)
		}
	}()
}

func (t *Topic) Send(m *message.Message) {
	if err := t.client.Send(context.Background(), m); err != nil {
		return
	}
}

type topicOption func(*Topic) error

func (c *Topic) applyOptions(opts ...topicOption) error {
	for _, fn := range opts {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}
