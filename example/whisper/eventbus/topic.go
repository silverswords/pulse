package main

import (
	"context"
	_ "github.com/silverswords/whisper/mq/eventbus"
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

	t, err := topic.NewTopic("eventbus:hello", *meta, topic.WithPubACK(), topic.WithCount())
	if err != nil {
		log.Fatal(err)
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
			//Sleep(Second)
			if count > 1e4 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("eventbus:hello", *meta, subscription.WithSubACK())
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
