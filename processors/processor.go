package processors

type Processor interface {
	Process(data []byte) error
}