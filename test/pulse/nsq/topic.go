package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"time"

	"github.com/silverswords/pulse/mq/nsq"
	"github.com/silverswords/pulse/pkg/components/mq"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[nsq.URL] = nsq.DefaultURL
	meta.Properties["DriverName"] = "nsq"

	//create a topic whose name is  "hello"
	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithCount())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		var count int
		for {
			count++
			//publish message to topic
			res := t.Publish(context.Background(), message.NewSimpleByteMessage([]byte(strconv.Itoa(count))))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			log.Println("send a message", count)
			time.Sleep(time.Second)
			if count > 1e7 {
				return
			}
		}
	}()

	//create a subscription to receive message
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
	err = s.Receive(context.Background(), func(ctx context.Context, m *message.CloudEventsEnvelope) {
		receiveCount++
		log.Println("receive the message:", m.ID, receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
