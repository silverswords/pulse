package whisper

import (
	"context"
	"github.com/silverswords/whisper/message"
	"time"
)

type Subscription struct {
	Driver
	topic      string
	queue      chan *message.Message
	subOptions []subOption

	handlerEndpoint []func(msg *message.Message) error
	callbackFn      interface{}
}

func NewSubscription() (*Subscription, error) {
	s := &Subscription{}
	receiver :=s.Driver.Subscribe()

	return s, nil
}

//
func (s *Subscription) startReceive() error {

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			m, err := s.Driver.Subscribe()

			for _, option := range s.handlerEndpoint {
				option(m)
			}
			callbackFn(m)
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
