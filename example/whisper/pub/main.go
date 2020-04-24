package main

import (
	"fmt"
	"github.com/silverswords/whisper"
	_ "github.com/silverswords/whisper/pubsub/nats"
	"runtime"
	"time"
)

func main(){
	c,err := whisper.Dial("nats",whisper.WithURL("nats://39.105.141.168:4222"),whisper.WithPrintMsg(),whisper.ACKsend(true))
	if err != nil {fmt.Println("===============================", err)}

	go func (){
		for i:= 0;i<10;i++{
			var Msg = whisper.Message{Header: nil, Body: []byte("hello"), ACK: uint64(i)}
			c.Send("msg/test", Msg)
			time.Sleep(2* time.Second)
		}
	}()

	for {
		c.Receive("msg/test")
		time.Sleep(1*time.Second)
	}

	runtime.Goexit()
}
