package whisper
// subscription hajack messages Do() to handle message with own thing instead of send by driver.
import "fmt"

type Subscription struct {
	*Executor  // use to handle message
	refused bool
}

func NewSubscription() *Subscription {
	return &Subscription{
		Executor: NewExecutor(simpleDriver{}),
	}
}

func (s *Subscription) Sub(topic string) error {
	d, ok:= s.Executor.Driver.(Driver)
	if !ok {
		return DriverError
	}

	return d.Sub(topic, func(message *Message) {
		s.receive(Receive(message))
	})
}

func (s *Subscription) receive(message Handler) error {
	// customization subscription like this.
	if s.refused {
		return nil
	}
	s.Executor.Append(message)
	return nil
}

// receiveMessage hajack messages Do() to and Do() selves.
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

