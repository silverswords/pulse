package main

import (
	"context"
	test "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"

	"log"
	"runtime"
	"strconv"
	"time"
)

func main() {
	meta := driver.NewMetadata()
	meta.Properties[nats.URL] =nats.DefaultURL
	// todo: SetDriver is need.
	meta.Properties["DriverName"] = "nats"

	// Connect to NATS
	nc, err := test.Connect(nats.DefaultURL)
	if err != nil {
		log.Println(err,nc)
	}
	t,err := whisper.NewTopic("hello",*meta, whisper.WithPubACK())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		for {
			t.Publish(context.Background(),whisper.NewMessage(strconv.FormatInt(time.Now().Unix(),10),[]byte("hello")))
			time.Sleep(1*time.Second)
		}
	}()

	s, err := whisper.NewSubscription("hello",*meta, whisper.WithSubACK())
	err = s.Receive(context.Background(),func(ctx context.Context, m *whisper.Message) {
		log.Println(m)
	})
	if err != nil {
		log.Println(err)
		return
	}

	runtime.Goexit()

}
