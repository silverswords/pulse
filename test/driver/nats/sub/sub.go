package main

import (
	na "github.com/nats-io/nats.go"
	"log"
	"runtime"
)

func main() {
	nc, err := na.Connect("nats://localhost:4222")
	if err != nil {
		return
	}
	var count int

	_, _ = nc.Subscribe("hello", func(msg *na.Msg) {
		log.Println(msg, count)
		count++
	})

	runtime.Goexit()
}
