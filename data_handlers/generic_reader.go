package datahandlers

type Decoder interface {
	NextToken() (any, error)
}

type GenericReader struct {
	decoder Decoder
}

func NewGenericReader(decoder Decoder) *GenericReader {
	return &GenericReader{
		decoder: decoder,
	}
}

func (dr *GenericReader) Read() (any, error) {
	return dr.decoder.NextToken()
}

func (dr *GenericReader) Cancel() error {
	return nil
}
