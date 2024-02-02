package datahandlers

type DataReader interface {
	Read() (any, error)
}

type DataWriter interface {
	Write(any) error
}

type DataProcessor interface {
	Process(any) (any, error)
}

// TODO maybe the steps in the pipeline should exchange information on channels to make
// them thread safe
type DataPipeline struct {
	reader    DataReader
	processor DataProcessor
	writer    DataWriter
}

func NewDataPipeline(r DataReader, p DataProcessor, w DataWriter) *DataPipeline {
	return &DataPipeline{
		reader:    r,
		processor: p,
		writer:    w,
	}
}

func (dp *DataPipeline) Run() error {
	for {
		data, err := dp.reader.Read()
		if err != nil {
			return err
		}

		// // exit successfully if we've reached the end of the input
		// if err == io.EOF {
		// 	return nil
		// }

		data, err = dp.processor.Process(data)
		if err != nil {
			return err
		}

		err = dp.writer.Write(data)
		if err != nil {
			return err
		}
	}
}

type DataReaderFactory interface {
	CreateReader() DataReader
}

type DataProcessorFactory interface {
	CreateProcessor() DataProcessor
}

type DataWriterFactory interface {
	CreateWriter() DataWriter
}

type DataPipelineFactory struct {
	reader    DataReaderFactory
	processor DataProcessorFactory
	writer    DataWriterFactory
}

func NewDataPipelineFactory(r DataReaderFactory, p DataProcessorFactory, w DataWriterFactory) *DataPipelineFactory {
	return &DataPipelineFactory{
		reader:    r,
		processor: p,
		writer:    w,
	}
}

func (dpf *DataPipelineFactory) CreatePipeline() *DataPipeline {
	return NewDataPipeline(dpf.reader.CreateReader(), dpf.processor.CreateProcessor(), dpf.writer.CreateWriter())
}
