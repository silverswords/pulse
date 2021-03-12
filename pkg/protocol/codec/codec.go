package codec

type Codec interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
