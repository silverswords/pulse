package main

import (
	"github.com/silverswords/whisper/pubsub/nats"
	"runtime"
)

func main() {
	nats.Sub("msg.test")
	runtime.Goexit()
}
