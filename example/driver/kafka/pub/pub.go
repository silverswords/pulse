package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	config := sarama.NewConfig()
	DefaultURL := []string{"192.168.0.251:9092"}
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(DefaultURL, config)
	if err != nil {
		log.Panic(err)
	}

	defer producer.Close()
	count := 0

	for {
		count++
		msg := &sarama.ProducerMessage{}
		msg.Topic = "hello"
		msg.Key = sarama.StringEncoder("miles")
		msg.Value = sarama.StringEncoder("hello,consumer")

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Panic(err)
		}
		fmt.Printf("count: %d Partition: %v offset: %v key: %v value: %v\n", count, partition, offset, msg.Key, msg.Value)
		time.Sleep(time.Second)
	}
}
