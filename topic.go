package whisper

import (
	"context"
	"github.com/silverswords/whisper/message"
)

type Topic struct {
	topic string
	topicOptions []topicOption
	client Client
	waittingMessage map[string]*message.Message
}

func NewTopic(topic string, options ...topicOption) (*Topic,error) {
	t := &Topic{topic: topic,topicOptions: options}

	if err := t.applyOptions(options...); err != nil {
		return nil,err
	}

	return t, nil
}

func (t *Topic) startSender() {
	go func() {
		for _, v := range t.queue {
			t.client.send()
		}
	}()
}

func (t *Topic) Send(m *message.Message) {
	if err := t.client.Send(context.Background(),m); err != nil{
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