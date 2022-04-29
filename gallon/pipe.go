package gallon

import (
	"io"
)

type Record []interface{}

type Pipe struct {
	buffer chan Record
	close  func() error
}

type Reader interface {
	Read(record *Record) error
	More() bool
}

func (r Pipe) Read(record *Record) error {
	select {
	case r := <-r.buffer:
		*record = r
	}

	return nil
}

func (r Pipe) More() bool {
	return true
}

type WriteCloser interface {
	Write(record Record) error
	Close() error
}

func (w Pipe) Write(record Record) error {
	select {
	case w.buffer <- record:
	}

	return nil
}

func (w Pipe) Close() error {
	close(w.buffer)
	return io.EOF
}

func NewPipe() Pipe {
	return Pipe{
		buffer: make(chan Record),
	}
}
