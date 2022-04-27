package outputfile

import (
	"context"
	"io"
	"os"
)

type FileFormat string

const (
	Json FileFormat = "json"
)

type OutputFileClientConfig struct {
	FilePath   string
	FileFormat FileFormat
}

type OutputFileClient struct {
	conf OutputFileClientConfig
	file *os.File
}

func (client *OutputFileClient) Connect(
	ctx context.Context,
) (io.Writer, error) {
	file, err := os.Create(client.conf.FilePath)
	if err != nil {
		return nil, err
	}
	client.file = file

	return client.file, nil
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
