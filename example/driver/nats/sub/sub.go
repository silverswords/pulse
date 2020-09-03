package main

import (
	na "github.com/nats-io/nats.go"
	"github.com/silverswords/pulse/mq/nats"
	"log"
	"runtime"
)

func main() {
	nc, err := na.Connect(nats.DefaultURL)
	if err != nil {
		return
	}
	var count int

	nc.Subscribe("hello", func(msg *na.Msg) {
		log.Println(msg, count)
		count++
	})

	runtime.Goexit()
}
