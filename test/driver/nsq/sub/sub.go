package main

import (
	"log"
	"runtime"

	ns "github.com/nsqio/go-nsq"
	"github.com/silverswords/pulse/drivers/nsq"
)

func main() {
	config := ns.NewConfig()
	//create a consumer to subscripe protocol
	r, _ := ns.NewConsumer("hello", "ch", config)

	var count int
	//add handler to handle protocol
	r.AddHandler(ns.HandlerFunc(func(message *ns.Message) error {
		count++
		log.Printf("Got a protocol: %s %d", message.Body, count)
		return nil
	}))

	//connect to nsqd
	err := r.ConnectToNSQD(nsq.DefaultURL)
	if err != nil {
		log.Println(err)
	}

	runtime.Goexit()
}
