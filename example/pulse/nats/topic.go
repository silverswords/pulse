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
	"strconv"
	"time"
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[nats.URL] = nats.DefaultURL
	meta.Properties["DriverName"] = "nats"

	start := time.Now()
	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(), topic.WithDebugCount())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), message.NewMessage([]byte(strconv.Itoa(count))))
			go func() {
				if err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			//log.Println("send a message", count)
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
		if receiveCount == 1e5-1000 {
			log.Println(time.Now().Sub(start))
		}
		log.Println("receive the message:", string(m.Data), receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()

}
