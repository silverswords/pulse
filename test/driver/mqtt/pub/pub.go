package main

import (
	MQTT "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	opts := MQTT.NewClientOptions()
	opts.AddBroker("tcp://127.0.0.1:1883")

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Publish("hello", 0, false, "hello"); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	client.Disconnect(250)
}
