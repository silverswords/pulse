package aresult

import (
	"context"
	"github.com/silverswords/pulse/pkg/visitor"
	"log"
)

// Result help to know error because of sending goroutine is another goroutine.
type Result struct {
	ready chan struct{}
	err   error
}

// NewResult help to know error because of sending goroutine is another goroutine.
func NewResult() *Result {
	return &Result{ready: make(chan struct{})}
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

func (r *Result) Set(err error) {
	r.err = err
	close(r.ready)
}

type AsyncResultActor struct {
	msg visitor.Visitor
	*Result
}

func NewAsyncResultActor(msg visitor.Visitor) *AsyncResultActor {
	return &AsyncResultActor{msg: msg, Result: &Result{ready: make(chan struct{}), err: nil}}
}

func (m *AsyncResultActor) Do(fn visitor.DoFunc) error {
	return m.msg.Do(func(ctx context.Context, r interface{}) error {
		log.Println("getting result")
		err := fn(ctx, r)
		m.Result.Set(err)
		log.Println("setted result")
		return err
	})
}
