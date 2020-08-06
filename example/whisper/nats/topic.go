package main

import (
	"context"
	"github.com/silverswords/whisper/mq/nats"
	"github.com/silverswords/whisper/pkg/components/mq"
	"github.com/silverswords/whisper/pkg/message"
	"github.com/silverswords/whisper/pkg/subscription"
	"github.com/silverswords/whisper/pkg/topic"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[nats.URL] = nats.DefaultURL
	meta.Properties["DriverName"] = "nats"

	t, err := topic.NewTopic("hello", *meta, topic.WithPubACK(), topic.WithCount())
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
				if err := res.Get(context.Background()); err != nil {
					log.Println("----------------------", err)
				}
			}()
			//log.Println("send a message", count)
			if count > 1e5 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", *meta, subscription.WithSubACK())
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
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()

}
