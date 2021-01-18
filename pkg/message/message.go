package message

type Msger interface {
	Msg() []byte
}

type Hookable interface {
	hooks() []byte
}
