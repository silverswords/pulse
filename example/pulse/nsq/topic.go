package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/silverswords/whisper/mq/nsq"
	"github.com/silverswords/whisper/pkg/components/mq"
	"github.com/silverswords/whisper/pkg/message"
	"github.com/silverswords/whisper/pkg/subscription"
	"github.com/silverswords/whisper/pkg/topic"
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[nsq.URL] = nsq.DefaultURL
	meta.Properties["DriverName"] = "nsq"

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
			log.Println("send a message", count)
			//time.Sleep(time.Second)
			if count > 1e3 {
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
