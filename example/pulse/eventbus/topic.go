package main

import (
	"context"
	_ "github.com/silverswords/pulse/mq/eventbus"
	"github.com/silverswords/pulse/pkg/components/mq"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/subscription"
	"github.com/silverswords/pulse/pkg/topic"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
)

func main() {
	meta := mq.NewMetadata()

	t, err := topic.NewTopic("eventbus:hello", *meta, topic.WithOrdered(), topic.WithRequiredACK(), topic.WithCount())
	//t, err := topic.NewTopic("eventbus:hello", *meta, topic.WithRequiredACK(), topic.WithCount())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), message.NewEventwithOrderKey([]byte(strconv.Itoa(count)), ""))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			//log.Println("send a message", count)
			//Sleep(Second)
			if count > 1e5 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("eventbus:hello", *meta, subscription.WithAutoACK())
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
		log.Println("receive the message:", string(m.Data), receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
