package visitor_test

import (
	"context"
	"fmt"
	. "github.com/silverswords/pulse/pkg/protocol"
	. "github.com/silverswords/pulse/pkg/visitor"
	"log"
	"reflect"
	"testing"
)

func TestDoFunc(t *testing.T) {
	var logPublisher = &NopPublisher{}
	err := annotations(
		annotations(&NameVisitor{}, "first"), "second").Do(logPublisher.Publish)
	if err != nil {
		t.Error(err)
	}
}

type Annotations struct {
	Visitor
	string
}

func annotations(actor Visitor, string string) *Annotations {
	return &Annotations{Visitor: actor, string: string}
}

func (a Annotations) Do(fn DoFunc) error {
	return a.Visitor.Do(func(ctx context.Context, r interface{}) error {
		log.Printf("doing %s pre ", a.string)
		defer func() {
			log.Printf("doing %s post ", a.string)
		}()

		return fn(ctx, r)
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
	Actor := NewRetryMessage(&Message{})
	// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
	publish := func(ctx context.Context, r interface{}) error {
		log.Println("this pre log to console: ", ctx, r)
		defer log.Println("this post log to console: ", ctx, r)
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
		Actor := WithRetry(3)(&NameVisitor{Name: "no content"})
		var p = &FailedHandler{MaxTimes: 3}

		err := Actor.Do(p.FailedDo)
		log.Println("err is: ", err)

		if err != nil {
			t.Error(err)
		}

	})
	t.Run("withRetry", func(t *testing.T) {
		wrapActor := Chain(WithRetry(3), WithRetry(3))(&NameVisitor{})
		var p = &FailedHandler{MaxTimes: 5}
		err := wrapActor.Do(p.FailedDo)
		if err != nil {
			t.Error(err)
		}
	})
}

type ExampleImplPublisher struct{}

// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
func (e *ExampleImplPublisher) Publish(ctx context.Context, r interface{}) error {
	req, ok := r.(*PublishRequest)
	if !ok {
		return fmt.Errorf("interface assert %s want: %v", reflect.TypeOf(r).String(), reflect.TypeOf(&PublishRequest{}))
	}
	message := req.Message
	log.Println(ctx, message)
	return nil
}

type NopPublisher struct{}

// Publish: warning: this publisher only pub protocol to stdout, so example does not work in real world.
func (e *NopPublisher) Publish(ctx context.Context, r interface{}) error {
	log.Println("this pre log to console: ", ctx, r)
	defer log.Println("this post log to console: ", ctx, r)
	return nil
}
