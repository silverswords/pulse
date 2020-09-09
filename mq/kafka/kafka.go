package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/silverswords/pulse/pkg/components/mq"
	"github.com/silverswords/pulse/pkg/logger"
)

const (
	DriverName = "kafka"
	URL        = "kafkaURL"
	DefaultURL = "192.168.0.251:9092"
)

func init() {
	// use to register the kafka to pubsub mq factory
	mq.Registry.Register(DriverName, func() mq.Driver {
		return NewKafka()
	})
	//log.Println("Register the kafka mq")
}

func NewKafka() *Driver {
	return &Driver{}
}

type Driver struct {
	metadata
	producer sarama.SyncProducer
}

type metadata struct {
	kafkaURL string
}

func parseKAFKAMetadata(meta mq.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[URL]; ok && val != "" {
		if m.kafkaURL, ok = val.(string); ok {
			return m, nil
		}
		return m, errors.New("kafka error: kafkaURL is not a string")
	}
	return m, errors.New("kafka error: missing kafka URL")
}

func (n *Driver) Init(metadata mq.Metadata) error {
	m, err := parseKAFKAMetadata(metadata)
	if err != nil {
		return err
	}

	n.metadata = m
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer([]string{DefaultURL}, config)
	n.producer = p
	if err != nil {
		return err
	}
	return nil
}

func (n *Driver) Publish(topic string, in []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(in),
	}

	n.producer.SendMessage(msg)

	return nil
}

func (n *Driver) Subscribe(topic string, handler func(msg []byte)) (mq.Closer, error) {
	Handler := func(msg *sarama.ConsumerMessage) error {
		handler(msg.Value)
		return nil
	}

	ctx, _ := context.WithCancel(context.Background())
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	cg, err := sarama.NewConsumerGroup([]string{DefaultURL}, "0", config)
	if err != nil {
		return nil, err
	}

	c := &consumer{
		ready:    make(chan bool),
		callback: Handler,
	}

	go func() {
		Logger := logger.NewLogger("logger")
		defer func() {
			Logger.Debugf("Closing ConsumerGroup for topics: %v", topic)
			err := cg.Close()

			if err != nil {
				Logger.Errorf("Error closing consumer group: %v", err)
			}
		}()

		for {
			Logger.Debugf("Subscribed and listening to topics: %s", topic)

			err = cg.Consume(ctx, []string{topic}, c)
			if err != nil {
				fmt.Println("consume err =", err)
			}
		}

	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	<-c.ready
	return nil, nil
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

func (n *Driver) Close() error {
	producer := n.producer
	producer.Close()
	return nil
}
