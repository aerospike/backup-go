package decoder

type Decoder interface {
	NextToken() (any, error)
}

type DecoderFactory interface {
	CreateDecoder() (Decoder, error)
}
