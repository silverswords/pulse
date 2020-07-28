package main

import (
	"fmt"
	"github.com/silverswords/whisper"
)

func main() {
	var y int
	m := whisper.NewMessage("ddd", []byte{'2'})
	m.Attributes.Add("haha", "haha")
	y += 1
	b := whisper.ToByte(m)
	fmt.Println(*m)
	msg, err := whisper.ToMessage(b)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(*msg)
}
