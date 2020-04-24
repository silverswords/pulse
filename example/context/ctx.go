package main

import (
	"context"
	"fmt"
)

func main() {
	ctx:=context.Background()
	v:= ctx.Value("hello")
	v, ok := v.(context.Context)
	if !ok {
		fmt.Println("ok")
	}
	fmt.Println(v,v.(context.Context))
}

