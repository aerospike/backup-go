package backup

import (
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

type ProcessorWrap[T models.TokenConstraint] struct {
	execs []pipe.Processor[T]
}

func NewProcessorWrap[T models.TokenConstraint](execs ...pipe.Processor[T]) pipe.ProcessorCreator[T] {
	return func() pipe.Processor[T] {
		return &ProcessorWrap[T]{
			execs: execs,
		}
	}
}

func (p *ProcessorWrap[T]) Process(data T) (T, error) {
	var err error
	for _, processor := range p.execs {
		data, err = processor.Process(data)
		if err != nil {
			return data, err
		}
	}

	return data, nil
}
