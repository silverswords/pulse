package message

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/message/protocol/retry"
	"github.com/silverswords/pulse/pkg/message/protocol/timingwheel"
	"log"
	"time"
)

type DoFunc func(interface{}, context.Context, error) error

type Actor interface {
	Do(DoFunc) error
}

// Below middleware code from go-kit endpoint. https://github.com/go-kit/kit/blob/master/endpoint/endpoint.go
type Middleware func(Actor) Actor

// Chain is a helper function for composing middlewares. Requests will
// traverse them in the order they're declared. That is, the first middleware
// is treated as the outermost middleware.
func Chain(outer Middleware, others ...Middleware) Middleware {
	return func(next Actor) Actor {
		for i := len(others) - 1; i >= 0; i-- { // reverse
			next = others[i](next)
		}
		return outer(next)
	}
}

type NopActor struct{}

func (nop *NopActor) Do(fn DoFunc) error {
	log.Println("nop doing pre")
	defer log.Println("nop doing post")
	return fn(nop, context.Background(), nil)
}

type Message struct {
	Data []byte

	Topic       string
	OrderingKey string
}

func (m *Message) Do(fn DoFunc) error {
	err := fn(m, context.Background(), nil)
	return err
}

type RetryActor struct {
	actor Actor
	*retry.Params
	//noRetryErr []error
}

func NewRetryMessage(msg Actor) *RetryActor {
	return &RetryActor{actor: msg}
}

func (m *RetryActor) Do(fn DoFunc) error {
	return m.actor.Do(func(r interface{}, ctx context.Context, err error) error {
		if err != nil {
			log.Println("[Cancel Retry]: oh, error before do something")
			return err
		}
		times := 0
		for {
			if err := m.actor.Do(fn); err == nil {
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

//DelayActor take an timingwheel and use it for its own delay setting.
type DelayActor struct {
	msg Actor
	*timingwheel.TimingWheel
	time.Duration
}

func NewDelayActor(msg Actor, timingWheel *timingwheel.TimingWheel) *DelayActor {
	return &DelayActor{msg: msg, TimingWheel: timingWheel}
}

func (d DelayActor) Do(doFunc DoFunc) error {
	return d.msg.Do(func(r interface{}, ctx context.Context, err error) error {
		ch := d.After(d.Duration)
		select {
		case <-ctx.Done():
			return errors.New("no enough")
		case <-ch:
			return doFunc(r, ctx, err)
		}
	})
}

type AsyncResultActor struct {
	msg Actor
	*Result
}

func NewAsyncResultActor(msg Actor) *AsyncResultActor {
	return &AsyncResultActor{msg: msg}
}

func (m *AsyncResultActor) Do(fn DoFunc) error {
	return m.msg.Do(func(r interface{}, ctx context.Context, err error) error {
		if err != nil {
			log.Println("err")
		}
		err = fn(r, ctx, nil)
		if err != nil {
			m.Result.set(err)
			return err
		}
		m.Result.set(nil)
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
