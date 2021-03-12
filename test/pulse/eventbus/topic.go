package main

import (
	"context"
	_ "github.com/silverswords/pulse/drivers/eventbus"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
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
	meta := driver.NewMetadata()

	t, err := topic.NewTopic("eventbus:hello", *meta, topic.WithOrdered(), topic.WithRequiredACK(), topic.WithCount())
	//t, err := topic.NewTopic("eventbus:hello", *meta, topic.WithRequiredACK(), topic.WithCount())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), protocol.NewSimpleByteMessage([]byte(strconv.Itoa(count))))
			go func() {
				if _, err := res.Get(context.Background()); err != nil {
					log.Println(err)
				}
			}()
			//log.Println("send a protocol", count)
			time.Sleep(time.Second)
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
	err = s.Receive(context.Background(), func(ctx context.Context, m *protocol.CloudEventsEnvelope) {
		receiveCount++
		log.Println("receive the protocol:", string(m.Data), receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}
