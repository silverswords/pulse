package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/silverswords/pulse/pkg/protocol"
	nats "github.com/silverswords/pulse/pkg/pubsub/driver/drivers/natsstreaming"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

func main() {
	meta := protocol.NewMetadata()
	meta.SetDriver(nats.DriverName)
	meta.Properties[nats.NatsURL] = "nats://localhost:4222"
	meta.Properties[nats.NatsStreamingClusterID] = "test-cluster"
	meta.Properties[nats.SubscriptionType] = "topic"
	meta.Properties[nats.ConsumerID] = "app-test-a"

	t, err := topic.NewTopic(meta)
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), protocol.NewMessage("test", "", []byte("hello")))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			time.Sleep(time.Second)
			//log.Println("send a protocol", count)
			if count > 1e5 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", meta, subscription.WithCount())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), protocol.NewSubscribeRequest("test", protocol.NewMetadata()), func(ctx context.Context, m *protocol.Message) {
		log.Println("receive message", m)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
