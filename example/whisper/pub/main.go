package main

import (
	"context"
	"github.com/silverswords/whisper")

func main(){
	c,err := whisper.Dial("nats")
	if err != nil {return}

	c.Send(context.Background(),whisper.Message{Header: nil,Body: []byte("hello")})

}
