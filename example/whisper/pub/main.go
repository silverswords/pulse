package main

import (
	"github.com/silverswords/whisper"
)

func main(){
	c,err := whisper.Dial("nats",whisper.WithURL("nats://39.105.141.168:4222"))
	if err != nil {return}

	c.Send("msg.test",whisper.Message{Header: nil,Body: []byte("hello")})

}
