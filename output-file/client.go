package outputfile

import (
	"compress/gzip"
	"context"
	"io"
	"os"
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
	file io.WriteCloser
}

func (client *OutputFileClient) Connect(
	ctx context.Context,
) (io.Writer, error) {
	filePath := client.conf.FilePath
	if client.conf.Compression == Gzip {
		filePath += ".gz"
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	var writer io.WriteCloser
	writer = file

	if client.conf.Compression == Gzip {
		writer = gzip.NewWriter(writer)
	}

	client.file = writer

	return writer, nil
}

func (client OutputFileClient) Close() error {
	if err := client.file.Close(); err != nil {
		return err
	}

	return nil
}

func NewOutputFileClient(conf OutputFileClientConfig) OutputFileClient {
	return OutputFileClient{
		conf: conf,
		file: nil,
	}
}
