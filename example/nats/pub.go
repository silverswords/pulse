package main

import (
	"github.com/silverswords/whisper/pubsub"
)

func main(){
	pubsub.Send("msg.test", []byte("hello"))

}