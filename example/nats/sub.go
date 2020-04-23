package main

import (
	"github.com/silverswords/whisper/pubsub"
	"runtime"
)

func main(){
	pubsub.Sub("msg.test")
	runtime.Goexit()
}