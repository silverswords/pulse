package whisper

type message struct {
	Header map[string]string
	Body []byte
	ACK uint64
}