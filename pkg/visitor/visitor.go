package visitor

import (
	"context"
	"errors"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/protocol/retry"
	"github.com/silverswords/pulse/pkg/protocol/timingwheel"

	"time"
)

var log = logger.NewLogger("pulse.enhancer")

func init() {
	log.SetOutputLevel(logger.InfoLevel)
}

type DoFunc func(ctx context.Context, request interface{}) (err error)

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
	log.Debug("nop doing pre")
	defer log.Debug("nop doing post")
	return fn(context.Background(), nop)
}

type FailedHandler struct {
	CallTimes int
	MaxTimes  int
}

func (fh *FailedHandler) FailedDo(context.Context, interface{}) error {
	fh.CallTimes++
	log.Debugf("failed doing %d times", fh.CallTimes)
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
	return m.actor.Do(func(ctx context.Context, r interface{}) error {
		cancelCtx, cancelFunc := context.WithCancel(ctx)
		err := fn(cancelCtx, r)
		if m.NoRetry(err) {
			log.Debug("[Cancel Retry]: oh, no need to retry", r)
			cancelFunc()
			return err
		}
		cancelFunc()

		times := 1
		for {
			log.Debug("enter retry loop")
			times++
			cancelCtx, cancelFunc := context.WithCancel(ctx)
			if err = fn(cancelCtx, r); err == nil {
				cancelFunc()
				log.Infof("[Successful Retry]: after %d times tried", times)
				return nil
			}
			cancelFunc()
			// every internal time
			err = m.Backoff(ctx, times)
			switch err {
			case nil:
				continue
			case retry.ErrCancel:
				log.Errorf("[Failed Retry]: after %d times tried", times)
				return err
			case retry.ErrMaxRetry:
				log.Errorf("[Failed Retry]: after %d times tried", times)
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
	return d.msg.Do(func(ctx context.Context, r interface{}) error {
		ch := d.After(d.Duration)
		select {
		case <-ctx.Done():
			return errors.New("no enough")
		case <-ch:
			return doFunc(ctx, r)
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
