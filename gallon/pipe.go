package gallon

import (
	"encoding/json"
	"io"
)

type Record []interface{}

type Reader struct {
	decoder *json.Decoder
}

func (r Reader) Read(record *Record) error {
	return r.decoder.Decode(record)
}

func (r Reader) More() bool {
	return r.decoder.More()
}

func NewReader(reader io.Reader) Reader {
	return Reader{
		decoder: json.NewDecoder(reader),
	}
}

type Writer struct {
	encoder *json.Encoder
	close   func() error
}

func (w Writer) Write(record Record) error {
	return w.encoder.Encode(record)
}

func (w Writer) Close() error {
	return w.close()
}

func NewWriter(writer io.Writer, close func() error) Writer {
	return Writer{
		encoder: json.NewEncoder(writer),
		close:   close,
	}
}

type Pipe struct {
	Reader Reader
	Writer Writer
}

func NewPipe() Pipe {
	reader, writer := io.Pipe()

	return Pipe{
		Reader: NewReader(reader),
		Writer: NewWriter(writer, writer.Close),
	}
}
