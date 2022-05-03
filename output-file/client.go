package outputfile

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"

	"github.com/myuon/ubiquitous-adventure/gallon"
	"github.com/rs/zerolog/log"
)

type FileFormat string

const (
	Jsonl FileFormat = "jsonl"
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
	Encoder     func(gallon.Record) ([]byte, error)
}

type OutputFileClient struct {
	conf OutputFileClientConfig
}

func (client OutputFileClient) Connect(
	ctx context.Context,
	reader gallon.Reader,
) error {
	filePath := client.conf.FilePath

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	var writer io.WriteCloser
	writer = file

	if client.conf.Compression == Gzip {
		writer = gzip.NewWriter(writer)
	}

	go func() {
		defer func() { writer.Close() }()
		var record gallon.Record

		for {
			if err := reader.Read(&record); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				log.Error().Err(err).Msg("failed to read record")
				continue
			}

			bs, err := client.conf.Encoder(record)
			if err != nil {
				log.Error().Err(err).Msg("failed to encode record")
				continue
			}

			if client.conf.FileFormat == Jsonl {
				bs = append(bs, '\n')
			}
			if _, err := writer.Write(bs); err != nil {
				log.Error().Err(err).Msg("failed to write record")
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

var _ gallon.OutputPlugin = OutputFileClient{}
