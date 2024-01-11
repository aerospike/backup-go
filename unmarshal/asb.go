package unmarshal

import (
	"fmt"
	"io"
)

type section int

const (
	invalidSection section = iota
	metadata
	global
	records
)

type CountingByteReader struct {
	io.ByteReader
	count uint64
}

func (c *CountingByteReader) ReadByte() (byte, error) {
	b, err := c.ByteReader.ReadByte()
	if err != nil {
		return 0, err
	}

	c.count++

	return b, err
}

type ASBReader struct {
	source        *CountingByteReader // TODO maybe replace this with an InputOpener that can open itself
	hasReadHeader bool
}

// TODO wrap errors returned from this with a character count
func (r *ASBReader) NextToken() (any, error) {
	if !r.hasReadHeader {
		return readHeader(r.source)
		r.hasReadHeader = true
	}

	b, err := r.source.ReadByte()
	if err != nil {
		err = fmt.Errorf("read failed at character %d %w", r.source.count, err)
		return nil, err
	}

	switch b {
	case '#':
		return readMetadata(r.source)

	default:
		return nil, fmt.Errorf("read invalid asb line start character %s", b)
	}
}

type Header struct {
	version string
}

func readHeader(src io.ByteReader) (*Header, error) {
	versionTextLen := len("Version x.y\n")
	bytes := make([]byte, versionTextLen)
	for i := 0; i < versionTextLen; i++ {
		var err error
		bytes[i], err = src.ReadByte()
		if err != nil {
			return nil, err
		}
	}

	return &Header{
		version: string(bytes[7:10]),
	}, nil
}

type MetaData struct {
	namespace string
	first     bool
}

// TODO handle namespaces with escaped characters (e.g. space or line feed)
func readMetadata(src io.ByteReader) (*MetaData, error) {
	namespace, err := readNamespace(src)
	if err != nil {
		return nil, err
	}
}

func readNamespace(src io.ByteReader) (string, error) {
	b, err := src.ReadByte()
	if err != nil {
		return "", err
	}

	if b != '#' {
		return "", fmt.Errorf("invalid character %s in metadata section", b)
	}

	b, err = src.ReadByte()
	if err != nil {
		return "", err
	}

	if b != ' ' {
		return "", fmt.Errorf("invalid character %s in metadata section", b)
	}

	b, err = src.ReadByte()
	if err != nil {
		return "", err
	}

	firstFileLen := len("first-file")
	bytes := make([]byte, versionTextLen)
	for i := 0; i < firstFileLen; i++ {
		var err error
		bytes[i], err = src.ReadByte()
		if err != nil {
			return nil, err
		}
	}

}
