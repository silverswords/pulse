package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"

	"github.com/silverswords/pulse/drivers/nats"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

func main() {
	meta := protocol.NewMetadata()
	meta.Properties[nats.URL] = "nats://34.92.175.246:4222"
	meta.Properties["DriverName"] = "nats"

	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithOrdered())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), protocol.NewSimpleByteMessage([]byte(strconv.Itoa(count))))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			//log.Println("send a protocol", count)
			if count > 1e5 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", *meta, subscription.WithCount(), subscription.WithAutoACK())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), func(ctx context.Context, m *protocol.CloudEventsEnvelope) {

	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
