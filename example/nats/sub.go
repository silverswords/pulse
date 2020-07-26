package main

import (
	"github.com/silverswords/whisper/driver/nats"
	"runtime"
)

func main() {
	nats_deprecated.Sub("msg.test")
	runtime.Goexit()
}
