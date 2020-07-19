package whisper

type Topic struct {
	topic string
	topicOptions []topicOption
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