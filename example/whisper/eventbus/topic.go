package main

import (
	"context"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/driver"
	_ "github.com/silverswords/whisper/driver/eventbus"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	. "time"
)

func main(){
	meta := driver.NewMetadata()

	t, err := whisper.NewTopic("eventbus:hello",*meta,whisper.WithPubACK(), whisper.WithCount())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), whisper.NewMessage([]byte("hello")))
			go func() {
				if err := res.Get(context.Background()); err != nil {
					log.Println("----------------------", err)
				}
			}()
			//log.Println("send a message", count)
			Sleep(Second)
			if count > 1e2 {
				return
			}
		}
	}()

	s, err := whisper.NewSubscription("eventbus:hello", *meta, whisper.WithSubACK())
	if err != nil {
		log.Println(err)
		return
	}

	go func() {
		panic(http.ListenAndServe(":8080", nil))
	}()

	var receiveCount int
	//ctx, _ := context.WithTimeout(context.Background(),time.Second * 10)
	err = s.Receive(context.Background(), func(ctx context.Context, m *whisper.Message) {
		receiveCount++
		log.Println("receive the message:", m.Id, receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()
}