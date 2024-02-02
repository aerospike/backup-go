package datahandlers

type NOOPProcessor struct{}

// NewProcessor creates a new Processor
func NewNOOPProcessor() *NOOPProcessor {
	return &NOOPProcessor{}
}

// Process processes the data
func (p *NOOPProcessor) Process(data interface{}) (interface{}, error) {
	return data, nil
}

type NOOPProcessorFactory struct{}

// NewNOOPProcessorFactory creates a new ProcessorFactory
func NewNOOPProcessorFactory() *NOOPProcessorFactory {
	return &NOOPProcessorFactory{}
}

// CreateProcessor creates a new Processor
func (f *NOOPProcessorFactory) CreateProcessor() DataProcessor {
	return NewNOOPProcessor()
}
