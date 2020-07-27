package main

import (
	"github.com/silverswords/whisper/client"
	"github.com/silverswords/whisper/internal"
	"log"
	"time"
)

var msg = &client.Message{
	MsgID:  1000,
	Header: make(internal.Header),
	Body:   []byte("Hello"),
}

func main() {
	topic := client.NewLocalTopic()
	defer topic.Stop()
	// normally send messages
	msg.Header.SetTopic("hello")
	topic.Send(msg)

	suber := client.NewLocalSub()
	unsubHandler, err := suber.Sub("hello")
	if err != nil {
		log.Println(err)
	}
	defer unsubHandler.Unsubscribe()
	topic.Send(msg)

	time.Sleep(time.Second)

	topic.Send(msg)
	time.Sleep(time.Second)
}
