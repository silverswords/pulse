package multiChan

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/message"
	"io"
	"log"
	"strings"
	"sync"
)

var subscribers map[string]map[string]chan *message.Message = make(map[string]map[string]chan *message.Message)
var mutex sync.RWMutex

type Driver struct {
	incoming chan *message.Message

	subscriptions []subscriber
}

type subscriber struct {
	topic        string
	subscriberid string
}

func NewDriver(subscriptions []subscriber) (*Driver, error) {
	d := &Driver{
		incoming:      make(chan *message.Message),
		subscriptions: subscriptions,
	}

	return d, nil
}

func (d *Driver) Send(ctx context.Context, m *message.Message) error {
	mutex.RLock()
	if maps, found := subscribers[m.Topic()]; found {
		go func(msg *message.Message, dataChannelSlices map[string]chan *message.Message) {
			for _, ch := range dataChannelSlices {
				if ch == nil {
					continue
				}

				// discard the oldest message
				if len(ch) == cap(ch) {
					select {
					case _ = <-ch:
					default:
					}
				}
				ch <- msg
				log.Println("send ", msg, " to ", ch)
			}

		}(m, maps)
	}
	mutex.RUnlock()
	return nil
}

func (d Driver) Receive(ctx context.Context) (*message.Message, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil Context")
	}

	select {
	case <-ctx.Done():
		return nil, io.EOF
	case m, ok := <-d.incoming:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	}
}

func (d *Driver) startSubscriber(ctx context.Context, sub subscriber) error {
	mutex.Lock()
	if subscribers[sub.topic] == nil {
		subscribers[sub.topic] = make(map[string]chan *message.Message)
	}
	if subscribers[sub.topic][sub.subscriberid] == nil {
		subscribers[sub.topic][sub.subscriberid] = make(chan *message.Message, 100)
	}
	mutex.Unlock()

	var m *message.Message
	select {
	case m = <-subscribers[sub.topic][sub.subscriberid]:
	case <-ctx.Done():
		return errors.New("No message until ctx done")
	}
	d.incoming <- m
	return nil
}

func (d *Driver) Open(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := len(d.subscriptions)

	quit := make(chan struct{}, n)
	errc := make(chan error, n)

	for _, sub := range d.subscriptions {
		go func(ctx context.Context, sub subscriber) {
			err := d.startSubscriber(cctx, sub)
			if err != nil {
				errc <- err
			} else {
				quit <- struct{}{}
			}
		}(ctx, sub)
	}

	errs := []string(nil)
	for success := 0; success < n; success++ {
		var err error
		select {
		case <-ctx.Done():
			{
				success--
			}
		case err = <-errc:
		case <-quit:
		}
		if cancel != nil {
			cancel()
			cancel = nil
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	close(quit)
	close(errc)

	if errs == nil {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}
