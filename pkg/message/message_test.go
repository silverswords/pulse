package message_test

import (
	"context"
	"fmt"
	. "github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"log"
	"reflect"
	"testing"
)

func TestDoFunc(t *testing.T) {
	var logpub = &NopPublisher{}
	err := annotations(
		annotations(&NopActor{}, "first"), "second").Do(logpub.Publish)
	if err != nil {
		t.Error(err)
	}
}

type Annotations struct {
	Actor
	string
}

func annotations(actor Actor, string string) *Annotations {
	return &Annotations{Actor: actor, string: string}
}

func (a Annotations) Do(fn DoFunc) error {
	return a.Actor.Do(func(r interface{}, ctx context.Context, err error) error {
		log.Printf("doing %s pre ", a.string)
		defer func() {
			log.Printf("doing %s post ", a.string)
		}()

		return fn(r, ctx, err)
	})
}

func ExampleActor() {
	var m Message
	// warning: this publisher only pub message to console, so example does not work in real world.
	var p = &ExampleImplPublisher{}

	err := m.Do(p.Publish)
	log.Println("err is ", err)
}

func ExampleRetryActor() {
	Actor := NewAsyncResultActor(NewRetryMessage(&Message{}))

	load := func(r interface{}, ctx context.Context, err error) error {
		if err != nil {
			log.Println("not expected error: ", err)
			return err
		}
		req, ok := r.(*driver.PublishRequest)
		if !ok {
			return fmt.Errorf("interface assert %s error: %v", reflect.TypeOf(r).String(), err)
		}
		req.Message.OrderingKey = "hello,actor"
		return nil
	}

	asyncResult := Actor.Get(context.Background())
	err := Actor.Do(load)
	log.Println(err, asyncResult)
}

func TestActor(t *testing.T) {
	var m = &Message{}
	// warning: this publisher only pub message to console, so example does not work in real world.
	var p = &ExampleImplPublisher{}

	err := m.Do(p.Publish)
	log.Println("err is ", err)
	if err != nil {
		t.Error(err)
	}
}

type ExampleImplPublisher struct{}

// Publish: warning: this publisher only pub message to stdout, so example does not work in real world.
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

// Publish: warning: this publisher only pub message to stdout, so example does not work in real world.
func (e *NopPublisher) Publish(r interface{}, ctx context.Context, err error) error {
	log.Println("this log to console: ", ctx, r, err)
	return nil
}
