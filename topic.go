package whisper

type Topic struct {
	topic string
	topicOptions []topicOption
}

func NewTopic(topic string, options ...topicOption) (*Topic,error) {
	t := &Topic{topic: topic,topicOptions: options}

	if err := t.applyOptions(options...); err != nil {
		return nil,err
	}

	return t, nil
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