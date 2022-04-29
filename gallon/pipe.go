package gallon

import (
	"encoding/json"
	"io"
)

type Record []interface{}

type Pipe struct {
	decoder *json.Decoder
	encoder *json.Encoder
	close   func() error
}

type Reader interface {
	Read(record *Record) error
	More() bool
}

func (r Pipe) Read(record *Record) error {
	return r.decoder.Decode(record)
}

func (r Pipe) More() bool {
	return r.decoder.More()
}

type WriteCloser interface {
	Write(record Record) error
	Close() error
}

func (w Pipe) Write(record Record) error {
	return w.encoder.Encode(record)
}

func (w Pipe) Close() error {
	return w.close()
}

func NewPipe() Pipe {
	reader, writer := io.Pipe()

	return Pipe{
		decoder: json.NewDecoder(reader),
		encoder: json.NewEncoder(writer),
		close:   writer.Close,
	}
}
