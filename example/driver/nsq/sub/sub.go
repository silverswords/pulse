package main

import (
	"log"
	"runtime"

	ns "github.com/nsqio/go-nsq"
	"github.com/silverswords/whisper/mq/nsq"
)

func main() {
	config := ns.NewConfig()
	//create a consumer to subscripe message
	r, _ := ns.NewConsumer("hello", "ch", config)

	var count int
	//add handler to handle message
	r.AddHandler(ns.HandlerFunc(func(message *ns.Message) error {
		count++
		log.Printf("Got a message: %s %d", message.Body, count)
		return nil
	}))

	//connect to nsqd
	err := r.ConnectToNSQD(nsq.DefaultURL)
	if err != nil {
		log.Println(err)
	}

	runtime.Goexit()
}
