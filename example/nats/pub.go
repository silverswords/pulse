package main

import (
	"github.com/silverswords/whisper/pubsub/nats"
)

func main(){
	nats.Send("msg.test", []byte("hello"))

}