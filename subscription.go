package whisper

import "github.com/silverswords/whisper/message"

type Subscription struct {
	topic string
	queue chan *message.Message
	subOptions []subOption
	callbackFn interface{}
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
