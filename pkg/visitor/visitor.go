package visitor

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/protocol/retry"
	"github.com/silverswords/pulse/pkg/protocol/timingwheel"
	"log"
	"time"
)

type DoFunc func(interface{}, context.Context, error) error

type Visitor interface {
	Do(DoFunc) error
}

// Below middleware code from go-kit visitor. https://github.com/go-kit/kit/blob/master/endpoint/endpoint.go
type Middleware func(Visitor) Visitor

// Chain is a helper function for composing middlewares. Requests will
// traverse them in the order they're declared. That is, the first middleware
// is treated as the outermost middleware.
func Chain(outer Middleware, others ...Middleware) Middleware {
	return func(next Visitor) Visitor {
		for i := len(others) - 1; i >= 0; i-- { // reverse
			next = others[i](next)
		}
		return outer(next)
	}
}

type NopVisitor struct {
	Name string
}

func (nop *NopVisitor) Do(fn DoFunc) error {
	log.Println("nop doing pre")
	defer log.Println("nop doing post")
	return fn(nop, context.Background(), nil)
}

type FailedHandler struct {
	CallTimes int
}

func (fh *FailedHandler) FailedDo(interface{}, context.Context, error) error {
	fh.CallTimes++
	log.Printf("failed doing %d times", fh.CallTimes)
	if fh.CallTimes < 3 {
		return errors.New("please try next time")
	}
	return nil
}

type RetryActor struct {
	actor Visitor
	*retry.Params
	noRetryErr []error
}

func WithRetry(retrytimes ...int) Middleware {
	return func(actor Visitor) Visitor {
		return &RetryActor{actor: actor, Params: &retry.Params{Strategy: retry.BackoffStrategyLinear, MaxTries: 3, Period: 1 * time.Millisecond}}
	}
}

func NewRetryMessage(msg Visitor) *RetryActor {
	return &RetryActor{actor: msg, Params: &retry.Params{Strategy: retry.BackoffStrategyLinear, MaxTries: 3, Period: 1 * time.Millisecond}}
}

// IfRetry(err), when err == nil, not retry is true.
// Then check for m.noRetryErr slice. If you set error no need to retry.
func (m *RetryActor) NoRetry(err error) bool {
	if err == nil {
		return true
	}
	for _, v := range m.noRetryErr {
		if err == v {
			return true
		}
	}
	return false
}

func (m *RetryActor) Do(fn DoFunc) error {
	return m.actor.Do(func(r interface{}, ctx context.Context, err error) error {
		if err != nil {
			return errors.New("error before start to retry")
		}
		err = fn(r, ctx, err)
		if m.NoRetry(err) {
			log.Println("[Cancel Retry]: oh, no need to retry", r)
			return err
		}

		times := 1
		for {
			log.Println("enter retry loop")
			times++
			if err = fn(r, ctx, err); err == nil {
				log.Printf("[Successful Retry]: oh, no need to retry after %d times tried", times)
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
	msg Visitor
	*timingwheel.TimingWheel
	time.Duration
}

func NewDelayActor(msg Visitor, timingWheel *timingwheel.TimingWheel) *DelayActor {
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
	msg Visitor
	*Result
}

func NewAsyncResultActor(msg Visitor) *AsyncResultActor {
	return &AsyncResultActor{msg: msg, Result: &Result{ready: make(chan struct{}), err: nil}}
}

func (m *AsyncResultActor) Do(fn DoFunc) error {
	return m.msg.Do(func(r interface{}, ctx context.Context, err error) error {
		log.Println("getting result")
		if err != nil {
			log.Println("err")
		}
		err = fn(r, ctx, nil)
		m.Result.set(err)
		log.Println("setted result")
		return err
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

// Get returns the server-generated protocol ID and/or error result of a Publish call.
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
	msg Visitor
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
