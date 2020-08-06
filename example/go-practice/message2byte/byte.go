package main

import (
	"fmt"
	"github.com/silverswords/whisper/pkg/message"
)

func main() {
	var y int
	m := message.NewMessage("ddd", []byte{'2'})
	m.Attributes.Add("haha", "haha")
	y += 1
	b := message.ToByte(m)
	fmt.Println(*m)
	msg, err := message.ToMessage(b)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(*msg)
}
