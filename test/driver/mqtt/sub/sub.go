package main

import (
	"fmt"
	"runtime"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	opts := MQTT.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:1883")

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", msg.Topic(), string(msg.Payload()))
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("hello", 0, nil); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	runtime.Goexit()
}
