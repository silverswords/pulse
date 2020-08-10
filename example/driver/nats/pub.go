package main

import (
	na "github.com/nats-io/nats.go"
	"github.com/silverswords/whisper/mq/nats"
	"log"
)

func main() {
	nc, err := na.Connect(nats.DefaultURL)
	if err != nil {
		log.Println(err)
		return
	}
	var count int
	for {
		count++
		log.Println(count)
		nc.Publish("hello", []byte("world"))
	}

}
