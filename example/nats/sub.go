package main

import (
	"github.com/silverswords/whisper/driver/nats"
	"runtime"
)

func main() {
	nats.Sub("msg.test")
	runtime.Goexit()
}
