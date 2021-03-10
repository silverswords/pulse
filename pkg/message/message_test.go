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
