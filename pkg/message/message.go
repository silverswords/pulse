package message

import (
	"context"
	"github.com/silverswords/pulse/pkg/driver"
	"github.com/silverswords/pulse/pkg/message/protocol/retry"
	"log"
)

type DoFunc func(*Message, context.Context, error) error

type Actor interface {
	Do(DoFunc) error
}

type Message struct {
	OrderingKey string
}

func (m *Message) Do(fn DoFunc) error {
	return fn(m, context.Background(), nil)
}

func PublishMessage(driver driver.Publisher, m *Message) error {
	return m.Do(func(m *Message, ctx context.Context, err error) error {
		return driver.Publish("topic from message", []byte{' ', ' '})
	})
}

func ExampleDo() {
	var m Message
	var pub driver.Publisher

	err := PublishMessage(pub, &m)
	log.Println("err is ", err)
}

type RetryActor struct {
	msg Actor
	*retry.Params
	//noRetryErr []error
}

func NewRetryMessage(msg Actor) *RetryActor {
	return &RetryActor{msg: msg}
}

func (m *RetryActor) Do(fn DoFunc) error {
	return m.msg.Do(func(msg *Message, ctx context.Context, err error) error {
		if err != nil {
			log.Println("[Cancel Retry]: oh, error before do something")
			return err
		}
		times := 0
		for {
			if err := m.msg.Do(fn); err == nil {
				return nil
			}
			// every internal time
			err = m.Backoff(ctx, times)
			if err != nil && err == retry.ErrCancel {
				return err
			} else if err == retry.ErrMaxRetry {
				continue
			}
		}
	})
}

func ExampleAsyncRetryActor() {
	Actor := NewAsyncResultActor(NewRetryMessage(&Message{}))

	load := func(msg *Message, ctx context.Context, err error) error {
		if err != nil {
			log.Println("not expected error: ", err)
			return err
		}
		msg.OrderingKey = "hello,actor"
		return nil
	}

	asyncResult := Actor.res.Get(context.Background())
	err := Actor.Do(load)
	log.Println(err, asyncResult)
}

type AsyncResultActor struct {
	msg Actor
	res *Result
}

func NewAsyncResultActor(msg Actor) *AsyncResultActor {
	return &AsyncResultActor{msg: msg}
}

func (m *AsyncResultActor) Do(fn DoFunc) error {
	return m.msg.Do(func(msg *Message, ctx context.Context, err error) error {
		if err != nil {
			log.Println("err")
		}
		err = fn(msg, ctx, nil)
		if err != nil {
			m.res.set(err)
			return err
		}
		m.res.set(nil)
		return nil
	})

}

// Result help to know error because of sending goroutine is another goroutine.
type Result struct {
	ready chan struct{}
	err   error
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *Result) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish call.
// Get blocks until the Publish call completes or the context is done.
func (r *Result) Get(ctx context.Context) (err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return r.err
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.Ready():
		return r.err
	}
}

func (r *Result) set(err error) {
	r.err = err
	close(r.ready)
}

type Acker interface {
	Ack()
	Nack()
}

type AckMessage struct {
	msg Actor
}

func (a *AckMessage) Do(doFunc DoFunc) error {
	if ack, ok := a.msg.(Acker); ok {
		if err := a.msg.Do(doFunc); err != nil {
			ack.Ack()
		} else {
			ack.Nack()
		}
	}
	return a.msg.Do(doFunc)
}
