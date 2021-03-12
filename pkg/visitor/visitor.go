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

type NameVisitor struct {
	Name string
}

func (nop *NameVisitor) Do(fn DoFunc) error {
	log.Println("nop doing pre")
	defer log.Println("nop doing post")
	return fn(nop, context.Background(), nil)
}

type FailedHandler struct {
	CallTimes int
	MaxTimes  int
}

func (fh *FailedHandler) FailedDo(interface{}, context.Context, error) error {
	fh.CallTimes++
	log.Printf("failed doing %d times", fh.CallTimes)
	if fh.CallTimes <= fh.MaxTimes {
		return errors.New("please try next time")
	}
	return nil
}

type RetryActor struct {
	actor Visitor
	*retry.Params
	noRetryErr []error
}

func WithRetry(retryTimes int) Middleware {
	return func(actor Visitor) Visitor {
		return &RetryActor{actor: actor, Params: &retry.Params{Strategy: retry.BackoffStrategyLinear, MaxTries: retryTimes, Period: 1 * time.Millisecond}}
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
		cancelCtx, cancelFunc := context.WithCancel(ctx)
		err = fn(r, cancelCtx, err)
		if m.NoRetry(err) {
			log.Println("[Cancel Retry]: oh, no need to retry", r)
			cancelFunc()
			return err
		}
		cancelFunc()

		times := 1
		for {
			log.Println("enter retry loop")
			times++
			cancelCtx, cancelFunc := context.WithCancel(ctx)
			if err = fn(r, cancelCtx, err); err == nil {
				cancelFunc()
				log.Printf("[Successful Retry]: oh, no need to retry after %d times tried", times)
				return nil
			}
			cancelFunc()
			// every internal time
			err = m.Backoff(ctx, times)
			switch err {
			case nil:
				continue
			case retry.ErrCancel:
				return err
			case retry.ErrMaxRetry:
				return err
			// retry
			default:
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

// Acker is for QoS.
type Acker interface {
	Ack()
	Nack()
}

type AutoAckMessage struct {
	msg Visitor
}

func (a *AutoAckMessage) Do(doFunc DoFunc) error {
	err := a.msg.Do(doFunc)
	if ack, ok := a.msg.(Acker); ok {
		if err != nil {
			ack.Ack()
		} else {
			ack.Nack()
		}
	}
	return err
}