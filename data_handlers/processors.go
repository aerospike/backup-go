package datahandlers

// processors.go contains the implementations of the DataProcessor interface
// used by dataPipelines in the backuplib package

// **** NOOP Processor ****

// NOOPProcessor satisfies the DataProcessor interface
// It does nothing to the data
type NOOPProcessor struct{}

// NewNOOPProcessor creates a new NOOPProcessor
func NewNOOPProcessor() *NOOPProcessor {
	return &NOOPProcessor{}
}

// Process processes the data
func (p *NOOPProcessor) Process(data any) (any, error) {
	return data, nil
}

type NOOPProcessorFactory struct{}
