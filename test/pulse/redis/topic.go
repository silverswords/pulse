package main

import (
	"context"
	"github.com/silverswords/pulse/drivers/redis"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"time"
)

func main() {
	meta := protocol.NewMetadata()
	meta.Properties[redis.URL] = redis.DefaultURL
	meta.Properties["DriverName"] = "redis"

	t, err := topic.NewTopic("hello", *meta,
		//topic.WithCount(),
		topic.WithRequiredACK(), topic.WithOrdered())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			_ = t.Publish(context.Background(), protocol.NewSimpleByteMessage([]byte(strconv.Itoa(count))))
			//go func() {
			//	if _, err := res.Get(context.Background()); err != nil {
			//		log.Println(err)
			//	}
			//}()
			//log.Println("send a protocol", count)
			time.Sleep(10 * time.Millisecond)
			if count > 1e7 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", *meta,
		//subscription.WithCount(),
		subscription.WithAutoACK())
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
