package message

type Messageable interface {
	value() []byte
}

type Hookable interface {
	hooks() []byte
}
