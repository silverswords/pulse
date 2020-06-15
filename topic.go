package whisper

import (
	"log"
)

type Topic struct{
	topicOptions []SendOptions
	sender *Sender
	sendFunc func(msg *Message)
}

type SendOptions func(func(msg *Message)) func(msg *Message)

func (t *Topic)Send(msg *Message) {
	t.sendFunc(msg)
}

func (t *Topic)Stop() {
	t.sender.Stop()
}

func NewTopic(driver Driver) *Topic {
	t:= &Topic{sender: NewSender(driver)}
	t.sendFunc = t.sender.Send
	for _,v := range t.topicOptions {
		t.sendFunc = v(t.sendFunc)
	}
	return t
}

// localtopic log msg with default options.
func NewLocalTopic() *Topic{
	t:= NewTopic(LoopbackDriver)
	t.topicOptions = append(t.topicOptions,LogOption)
	return t
}

func LogOption(next func(msg *Message)) func(msg *Message) {
	return func(msg *Message) {
		log.Println("This is Test for Send Handler --- send: ",msg)
		next(msg)
	}
}