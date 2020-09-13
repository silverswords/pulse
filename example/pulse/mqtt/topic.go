package main

import (
	"context"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/silverswords/pulse/mq/mqtt"
	"github.com/silverswords/pulse/pkg/components/mq"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[mqtt.URL] = mqtt.DefaultURL
	meta.Properties["DriverName"] = "mqtt"
	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithCount())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), message.NewMessage([]byte("hello")))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println("----------------------", err)
				}
			}()
			log.Println("send a message", count)
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
	err = s.Receive(context.Background(), func(ctx context.Context, m *message.Message) {
		receiveCount++
		log.Println("receive the message:", m.Id, receiveCount)
		log.Println(m)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
