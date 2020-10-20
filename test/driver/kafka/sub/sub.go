package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {
	DefaultURL := "192.168.0.251:9092"
	ctx, cancel := context.WithCancel(context.Background())
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	cg, err := sarama.NewConsumerGroup([]string{DefaultURL}, "0", config)
	if err != nil {
		log.Panic(err)
	}
	count := 0
	c := &consumer{
		ready: make(chan bool),
		callback: func(msg *sarama.ConsumerMessage) error {
			count++
			fmt.Printf("count: %d Partition: %d, offset:%v key:%v value:%v \n", count, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			return nil
		},
	}

	err = cg.Consume(ctx, []string{"hello"}, c)
	if err != nil {
		log.Println(err)
	}
	cancel()
}

type consumer struct {
	ready    chan bool
	callback func(msg *sarama.ConsumerMessage) error
	once     sync.Once
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if consumer.callback != nil {
			err := consumer.callback(&sarama.ConsumerMessage{
				Topic: claim.Topic(),
				Value: message.Value,
			})
			if err == nil {
				session.MarkMessage(message, "")
			}
		}
	}

	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	consumer.once.Do(func() {
		close(consumer.ready)
	})

	return nil
}
