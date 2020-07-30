package main

import (
	na"github.com/nats-io/nats.go"
	"github.com/silverswords/whisper/driver/nats"
	"log"
	"time"
)

func main() {
	nc, err := na.Connect(nats.DefaultURL)
	if err != nil {
		return
	}
	var count int
	for {
		count ++
		log.Println(count)
		nc.Publish("hello",[]byte("world"))
		time.Sleep(time.Microsecond)
	}

}