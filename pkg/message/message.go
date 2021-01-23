package message

type Msger interface {
	Msg() []byte
}

type VisitorFunc func(*Message, error) error

type Visitor interface {
	Visit(VisitorFunc)
}

type Message struct {
}

func (m *Message) Visit(fn VisitorFunc) error {
	return fn(m, nil)
}

type EventMessageVisitor struct {
	Message
}

func (e *EventMessageVisitor) Visit(fn VisitorFunc) error {
	return e.Message.Visit(func(m *Message, err error) error {
		err = fn(m, err)
		if err == nil {

		}
		return nil
	})
}

type Hookable interface {
	hooks() []byte
}
