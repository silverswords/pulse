package main

import (
	"log"
	"time"

	ns "github.com/nsqio/go-nsq"
	"github.com/silverswords/pulse/pkg/pubsub/driver/drivers/nsq"
)

func main() {
	config := ns.NewConfig()
	//create a new producer to publish protocol
	w, err := ns.NewProducer(nsq.DefaultURL, config)
	if err != nil {
		log.Panic(err)
	}

	count := 0
	for {
		//count the number of protocol that be published
		count++
		log.Println(count)
		//publish messages
		err := w.Publish("hello", []byte("world"))
		if err != nil {
			log.Panic(err)
		}
		time.Sleep(time.Second)
	}
}
