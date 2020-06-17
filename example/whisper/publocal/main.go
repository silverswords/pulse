package main

import (
	"github.com/silverswords/whisper"
	"log"
	"time"
)

var msg = &whisper.Message{
	MsgID:  1000,
	Header: make(whisper.Header),
	Body:   []byte("Hello"),
}

func main() {
	topic := whisper.NewLocalTopic()
	defer topic.Stop()
	// normally send messages
	msg.Header.SetTopic("hello")
	topic.Send(msg)

	suber := whisper.NewLocalSub()
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
