package whisper

import (
	"log"
)

type Subscription struct {
	*Queue                // use to store messages
	handlers []SubHandler // use to handle messages
	Driver
}

type SubHandler interface {
	Do(*Message) error
}

type SubHandlerFunc func(*Message) error

func (h SubHandlerFunc) Do(m *Message) error { return h(m) }

func NewSubscription(driver Driver) *Subscription {
	return &Subscription{
		Queue:  NewQueue(),
		Driver: driver,
	}
}

func (s *Subscription) AddHandler(handler func(*Message) error) {
	s.handlers = append([]SubHandler{SubHandlerFunc(handler)}, s.handlers...)
}

func NewLocalSub() *Subscription {
	sub := NewSubscription(LoopbackDriver)
	sub.AddHandler(LogMessage)
	return sub
}

func LogMessage(m *Message) error {
	log.Println("This is test for SubHandler: receive: ", m)
	return nil
}

// Sub would sub multiple topic and do the same action with SubHandlers.
func (s *Subscription) Sub(topic string) (UnSubscriber, error) {
	unsub, err := s.Driver.Sub(topic, func(message *Message) {
		log.Println("get a message and append in subscription: ", message)
		s.Append(message)
	})
	if err != nil {
		return nil, err
	}

	go func() {
		for unsub != nil {
			msg := s.Pop().(*Message)
			for _, v := range s.handlers {
				err := v.Do(msg)
				if err != nil {
					log.Println("err in handle message: ", err)
				}
			}
		}
		// below for debug
		if unsub == nil {
			log.Println("suber already closed")
		}
	}()
	return unsub, nil
}

//// receiveMessage hajack messages Do() to and Do() selves.
//type receiveMessage struct {
//	*Message
//}
//
//func Receive(message *Message) Handler {
//	return &receiveMessage{
//		message,
//	}
//}
//
//func (r *receiveMessage) Do(driver interface{}) error {
//	fmt.Println("Get message and handle message with driver or do something.")
//	return nil
//}
