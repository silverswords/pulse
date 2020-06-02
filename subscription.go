package whisper

import "fmt"

type Subscription struct {
	subDriver
	*Executor
	retry bool
}

func NewSubscription() *Subscription {
	return &Subscription{
		Executor: NewExecutor(simpleDriver{}),
	}
}

func (s *Subscription) append(message Handler) error {
	// customization subscription like this.
	if s.retry {
		s.Executor.Append(Retry(message))
	}
	return nil
}

func (s *Subscription) Sub(topic string) error {
	return s.subDriver.Sub(topic, Receive)
}

type subDriver interface {
	Sub(topic string, handler func(message *Message) Handler)error
}

type receiveMessage struct {
	*Message
}

func Receive(message *Message) Handler {
	return &receiveMessage{
		message,
	}
}

func (r *receiveMessage) Do(driver interface{}) error {
	fmt.Println("Get message and handle message with driver or do something.")
	return nil
}

