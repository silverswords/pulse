package main

import (
	"github.com/silverswords/whisper/driver/nats"
)

func main() {
	nats_deprecated.Send("msg.test", []byte("hello"))

}
