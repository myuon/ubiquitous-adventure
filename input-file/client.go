package inputfile

import (
	"bufio"
	"context"
	"os"

	"github.com/myuon/ubiquitous-adventure/gallon"
	"github.com/rs/zerolog/log"
)

type FileFormat string

const (
	Jsonl FileFormat = "jsonl"
)

type InputFileClientConfig struct {
	FilePath   string
	FileFormat FileFormat
	Decoder    func([]byte) (gallon.Record, error)
}

type InputFileClient struct {
	conf InputFileClientConfig
}

func (client *InputFileClient) Connect(
	ctx context.Context,
	writer gallon.WriteCloser,
) error {
	file, err := os.Open(client.conf.FilePath)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(file)

	go func() {
		defer func() {
			if err := writer.Close(); err != nil {
				log.Error().Err(err).Msg("failed to close writer")
			}
			if err := file.Close(); err != nil {
				log.Error().Err(err).Msg("failed to close file")
			}
		}()

		if client.conf.FileFormat == Jsonl {
			for scanner.Scan() {
				r, err := client.conf.Decoder(scanner.Bytes())
				if err != nil {
					log.Error().Err(err).Msg("failed to decode item")
					continue
				}

				if err := writer.Write(r); err != nil {
					log.Error().Err(err).Msg("failed to write item")
					continue
				}
			}
		} else {
			log.Error().Str("format", string(client.conf.FileFormat)).Msg("unknown file format")
		}
	}()

	return nil
}

func NewInputFileClient(conf InputFileClientConfig) InputFileClient {
	return InputFileClient{
		conf: conf,
	}
}
