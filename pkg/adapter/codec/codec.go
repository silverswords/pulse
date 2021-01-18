package codec

type Codec interface {
	Encode(v interface{})
	Decode(v interface{}) error
}
