package main

import (
	"fmt"
	"github.com/asaskevich/EventBus"
)

func calculator(a int, b int) {
	fmt.Printf("%d\n", a+b)
}
func main() {
	bus := EventBus.New()
	bus.Subscribe("main:calculator", calculator)
	bus.Subscribe("main:hell", calculator)
	bus.Publish("main:calculator", 20, 40)
	bus.Publish("main:hell", 20, 40)
	bus.Unsubscribe("main:calculator", calculator)
}
