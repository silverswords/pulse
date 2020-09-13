package main

import (
	"log"

	na "github.com/nats-io/nats.go"
)

func main() {
	nc, err := na.Connect("nats_client:W64f8c6vG6@192.168.0.253:31476")
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
