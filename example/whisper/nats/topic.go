package main

import (
	"context"
	test "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/driver/nats"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func main() {
	// ...
	//cpuProfile, _ := os.Create("cpu_profile")
	//pprof.StartCPUProfile(cpuProfile)
	//defer pprof.StopCPUProfile()
	//
	//f, err := os.Create("mem_profile")
	//pprof.WriteHeapProfile(f)
	//f.Close()


	meta := driver.NewMetadata()
	meta.Properties[nats.URL] = nats.DefaultURL
	meta.Properties["DriverName"] = "nats"

	// Connect to NATS
	nc, err := test.Connect(nats.DefaultURL)
	if err != nil {
		log.Println(err, nc)
	}
	t, err := whisper.NewTopic("hello", *meta, whisper.WithPubACK(),whisper.WithCount())
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		var count int
		for {
			count++
			res := t.Publish(context.Background(), whisper.NewMessage( []byte("hello")))
			go func() {
				if err := res.Get(context.Background()); err != nil {
					log.Println("----------------------",err)
				}
			}()
			//log.Println("send a message", count)
			if count > 1e5{
				return
			}
		}
	}()

	s, err := whisper.NewSubscription("hello", *meta, whisper.WithSubACK())
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
		receiveCount ++
		log.Println("receive the message:", m.Id, receiveCount)
	})

	if err != nil {
		log.Println(err)
		return
	}
	runtime.Goexit()

}
