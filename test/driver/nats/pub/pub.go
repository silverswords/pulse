package main

import (
	"log"

	na "github.com/nats-io/nats.go"
)

func main() {
	nc, err := na.Connect("nats://34.92.175.246:4222")
	if err != nil {
		log.Println(err)
		return
	}
	var count int
	for {
		count++
		log.Println(count)
		_ = nc.Publish("hello", []byte("world"))
	}
}
