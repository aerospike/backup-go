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

// GenericReaderFactory is a factory for creating GenericReaders
type GenericReaderFactory struct {
	decoder Decoder
}

func NewGenericReaderFactory(d Decoder) *GenericReaderFactory {
	return &GenericReaderFactory{
		decoder: d,
	}
}

func (f *GenericReaderFactory) CreateReader() DataReader {
	return NewGenericReader(f.decoder)
}
