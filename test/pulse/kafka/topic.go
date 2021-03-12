package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"time"

	_ "github.com/silverswords/pulse/drivers/eventbus"
	"github.com/silverswords/pulse/drivers/kafka"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

func main() {
	meta := protocol.NewMetadata()
	meta.Properties[kafka.URL] = kafka.DefaultURL
	meta.Properties["DriverName"] = "kafka"
	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithCount())
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
					log.Println("----------------------", err)
				}
			}()
			//log.Println("send a protocol", count)
			time.Sleep(time.Second)
			if count > 1e4 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", *meta, subscription.WithAutoACK())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	var receiveCount int
	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), func(ctx context.Context, m *protocol.CloudEventsEnvelope) {
		receiveCount++
		log.Println("receive the protocol:", m.ID, receiveCount)
		log.Println(m)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
