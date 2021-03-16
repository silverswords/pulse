package main

import (
	"context"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/visitor"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/silverswords/pulse/pkg/protocol"
	nats "github.com/silverswords/pulse/pkg/pubsub/driver/drivers/natsstreaming"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

var log = logger.NewLogger("pulse.natsstreaming.test")

func main() {
	log.SetOutputLevel(logger.InfoLevel)
	meta := protocol.NewMetadata()
	meta.SetDriver(nats.DriverName)
	meta.Properties[nats.NatsURL] = "nats://localhost:4222"
	meta.Properties[nats.NatsStreamingClusterID] = "test-cluster"
	meta.Properties[nats.SubscriptionType] = "topic"
	meta.Properties[nats.ConsumerID] = "app-test-a"

	t, err := topic.NewTopic(meta, topic.WithMiddlewares(visitor.WithRetry(3)))
	if err != nil {
		log.Error(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), protocol.NewMessage("test", "", []byte("hello")))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Error(err)
				}
			}()
			time.Sleep(time.Millisecond)
			if count%1e2 == 0 {
				log.Infof("Yeah, %d messages send", count)
			}
			//log.Println("send a protocol", count)
			if count > 1e4 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", meta, subscription.WithCount())
	if err != nil {
		log.Error(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	var count int
	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), protocol.NewSubscribeRequest("test", meta), func(ctx context.Context, m *protocol.Message) {
		log.Debug("receive message ", m)
		count++
		if count%1e4 == 0 {
			log.Infof("Yeah, %d messages received", count)
		}
	})

	if err != nil {
		log.Error(err)
		return
	}
	runtime.Goexit()
}
