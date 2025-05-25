package backup

import (
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

type dataProcessor[T models.TokenConstraint] struct {
	execs []pipe.Processor[T]
}

func newDataProcessor[T models.TokenConstraint](execs ...pipe.Processor[T]) pipe.ProcessorCreator[T] {
	return func() pipe.Processor[T] {
		return &dataProcessor[T]{
			execs: execs,
		}
	}
}

func (p *dataProcessor[T]) Process(data T) (T, error) {
	var err error
	for _, processor := range p.execs {
		data, err = processor.Process(data)
		if err != nil {
			return data, err
		}
	}

	return data, nil
}
