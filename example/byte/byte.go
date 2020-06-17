package main

import (
	"fmt"
)

type simple struct {
	x string
	b []byte
}

func main() {
	s := &simple{
		x: "hello",
	}

	fmt.Println(s)

	s.b = append(s.b, '2')
	fmt.Println(s)
}
