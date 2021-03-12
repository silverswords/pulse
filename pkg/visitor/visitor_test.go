package visitor_test

import (
	"context"
	"fmt"
	. "github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"github.com/silverswords/pulse/pkg/visitor"
	"log"
	"reflect"
	"testing"
)

func TestDoFunc(t *testing.T) {
	var logpub = &NopPublisher{}
	err := annotations(
		annotations(&visitor.NopVisitor{}, "first"), "second").Do(logpub.Publish)
	if err != nil {
		t.Error(err)
	}
}

type Annotations struct {
	visitor.Visitor
	string
}

func annotations(actor visitor.Visitor, string string) *Annotations {
	return &Annotations{Visitor: actor, string: string}
}

func (a Annotations) Do(fn visitor.DoFunc) error {
	return a.Visitor.Do(func(r interface{}, ctx context.Context, err error) error {
		log.Printf("doing %s pre ", a.string)
		defer func() {
			log.Printf("doing %s post ", a.string)
		}()

		return fn(r, ctx, err)
	})
}

func ExampleVisitor() {
	var m Message
	// warning: this publisher only pub protocol to console, so example does not work in real world.
	var p = &ExampleImplPublisher{}

	err := m.Do(p.Publish)
	log.Println("err is ", err)
}

func ExampleRetryActor() {
	Actor := visitor.NewRetryMessage(&Message{})
	// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
	publish := func(r interface{}, ctx context.Context, err error) error {
		log.Println("this pre log to console: ", ctx, r, err)
		defer log.Println("this post log to console: ", ctx, r, err)
		return nil
	}

	err := Actor.Do(publish)
	log.Println(err)
}

func TestActor(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		var m = &Message{}
		// warning: this publisher only pub protocol to console, so example does not work in real world.
		var p = &NopPublisher{}

		err := m.Do(p.Publish)
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("RetryActor", func(t *testing.T) {
		Actor := visitor.NewRetryMessage(&visitor.NopVisitor{Name: "no operation"})
		var p = &visitor.FailedHandler{}

		err := Actor.Do(p.FailedDo)
		log.Println("err is: ", err)

		if err != nil {
			t.Error(err)
		}

	})
	t.Run("withRetry", func(t *testing.T) {
		wrapActor := visitor.Chain(visitor.WithRetry(3), visitor.WithRetry(3))(&visitor.NopVisitor{})
		var p = &visitor.FailedHandler{}
		err := wrapActor.Do(p.FailedDo)
		if err != nil {
			t.Error(err)
		}
	})
}

type ExampleImplPublisher struct{}

// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
func (e *ExampleImplPublisher) Publish(r interface{}, ctx context.Context, err error) error {
	req, ok := r.(*driver.PublishRequest)
	if !ok {
		return fmt.Errorf("interface assert %s want: %v", reflect.TypeOf(r).String(), reflect.TypeOf(&driver.PublishRequest{}))
	}
	message := req.Message
	log.Println(ctx, message, err)
	return nil
}

type NopPublisher struct{}

// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
func (e *NopPublisher) Publish(r interface{}, ctx context.Context, err error) error {
	log.Println("this pre log to console: ", ctx, r, err)
	defer log.Println("this post log to console: ", ctx, r, err)
	return nil
}
