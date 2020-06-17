package whisper

type Driver interface {
	Pub(topic string, msg *Message) error
	Sub(topic string, handler func(*Message)) (UnSubscriber, error)
}

type UnSubscriber interface {
	//Drain() error
	Unsubscribe() error
}
