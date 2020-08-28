package main

import (
	"log"
	"time"

	ns "github.com/nsqio/go-nsq"
	"github.com/silverswords/whisper/mq/nsq"
)

func main() {
	config := ns.NewConfig()
	w, err := ns.NewProducer(nsq.DefaultURL, config)
	if err != nil {
		log.Panic(err)
	}
	count := 0
	for {
		count++
		log.Println(count)
		err := w.Publish("hello", []byte("world"))
		if err != nil {
			log.Panic(err)
		}
		time.Sleep(time.Second)
	}
}
