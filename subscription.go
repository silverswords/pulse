package whisper

import "github.com/silverswords/whisper/message"

type Subscription struct {
	Driver
	topic string
	queue chan *message.Message
	subOptions []subOption
	callbackFn interface{}
}

func NewSubscription() (*Subscription,error) {
	s := &Subscription{

	}


	return s, nil
}

func (s *Subscription) startReceive() error {
	go func() {
		for s.Driver.Receive() != nil {
			for option := range s.subOptions{+

			}
			callbackFn()
		}
	}()
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
