package datahandlers

import (
	"backuplib/models"
	"io"
)

type SIndexGetter interface {
	GetSIndexes(namespace string) ([]*models.SecondaryIndex, error)
}

type AerospikeSIndexReader struct {
	client       SIndexGetter
	namespace    string
	sindexes     []*models.SecondaryIndex
	sindexCursor int
}

func NewAerospikeSIndexReader(client SIndexGetter, namespace string) *AerospikeSIndexReader {
	return &AerospikeSIndexReader{
		client: client,
	}
}

func (r *AerospikeSIndexReader) Read() (any, error) {
	// grab all the sindexes on the first run
	if r.sindexes == nil {
		sindexes, err := r.client.GetSIndexes(r.namespace)
		if err != nil {
			return nil, err
		}
		r.sindexes = sindexes
	}

	if r.sindexCursor < len(r.sindexes) {
		sindex := r.sindexes[r.sindexCursor]
		r.sindexCursor++
		return sindex, nil
	}

	return nil, io.EOF
}

func (r *AerospikeSIndexReader) Cancel() error {
	return nil
}
