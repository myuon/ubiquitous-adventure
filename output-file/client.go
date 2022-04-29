package outputfile

import (
	"compress/gzip"
	"context"
	"io"
	"log"
	"os"

	"github.com/myuon/ubiquitous-adventure/gallon"
)

type FileFormat string

const (
	Json FileFormat = "json"
)

type Compression string

const (
	None Compression = "none"
	Gzip Compression = "gzip"
)

type OutputFileClientConfig struct {
	FilePath    string
	FileFormat  FileFormat
	Compression Compression
}

type OutputFileClient struct {
	conf OutputFileClientConfig
}

func (client *OutputFileClient) Connect(
	ctx context.Context,
	reader gallon.Reader,
	encoder func(gallon.Record) ([]byte, error),
) error {
	filePath := client.conf.FilePath
	if client.conf.Compression == Gzip {
		filePath += ".gz"
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	var writer io.WriteCloser
	defer func() { writer.Close() }()
	writer = file

	if client.conf.Compression == Gzip {
		writer = gzip.NewWriter(writer)
	}

	go func() {
		var record gallon.Record

		for reader.More() {
			if err := reader.Read(&record); err != nil {
				log.Fatalf("%v", err)
				continue
			}

			bs, err := encoder(record)
			if err != nil {
				log.Fatalf("%v", err)
				continue
			}
			if _, err := writer.Write(bs); err != nil {
				log.Fatalf("%v", err)
				continue
			}
		}
	}()

	return nil
}

func NewOutputFileClient(conf OutputFileClientConfig) OutputFileClient {
	return OutputFileClient{
		conf: conf,
	}
}
