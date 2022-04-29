package gallon

import (
	"io"
	"sync"
)

type Record []interface{}

type Pipe struct {
	buffer chan Record
	done   chan struct{}
	once   *sync.Once // for done
}

type Reader interface {
	Read(record *Record) error
}

func (r Pipe) Read(record *Record) error {
	select {
	case <-r.done:
		return io.EOF
	case r := <-r.buffer:
		*record = r
	}

	return nil
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
	w.once.Do(func() {
		close(w.done)
	})

	return nil
}

func NewPipe() Pipe {
	done := make(chan struct{})

	return Pipe{
		buffer: make(chan Record),
		done:   done,
		once:   &sync.Once{},
	}
}
