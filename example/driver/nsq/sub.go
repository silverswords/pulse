package main

import (
	"log"
	"runtime"

	ns "github.com/nsqio/go-nsq"
	"github.com/silverswords/whisper/mq/nsq"
)

func main() {
	config := ns.NewConfig()
	r, _ := ns.NewConsumer("hello", "ch", config)
	var count int

	r.AddHandler(ns.HandlerFunc(func(message *ns.Message) error {
		count++
		log.Printf("Got a message: %s %d", message.Body, count)
		return nil
	}))
	err := r.ConnectToNSQD(nsq.DefaultURL)
	if err != nil {
		log.Println(err)
	}
	runtime.Goexit()
}
