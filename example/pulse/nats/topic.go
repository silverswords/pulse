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
)

func main() {
	meta := mq.NewMetadata()
	meta.Properties[nats.URL] = nats.DefaultURL
	meta.Properties["DriverName"] = "nats"

	t, err := topic.NewTopic("hello", *meta, topic.WithRequiredACK(),topic.WithOrdered())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), message.NewEventwithOrderKey([]byte(strconv.Itoa(count)),"1"))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			//log.Println("send a message", count)
			if count > 1e5 {
				return
			}
		}
	}()

	s, err := subscription.NewSubscription("hello", *meta,subscription.WithCount(), subscription.WithAutoACK())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), func(ctx context.Context, m *message.Message) {

	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
