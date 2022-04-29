package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/myuon/ubiquitous-adventure/gallon"
	inputdynamodb "github.com/myuon/ubiquitous-adventure/input-dynamodb"
	outputfile "github.com/myuon/ubiquitous-adventure/output-file"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

type InData struct {
	Id        string                `dynamodbav:"id"`
	UserId    string                `dynamodbav:"user_id"`
	GachaType string                `dynamodbav:"gacha_type"`
	CreatedAt attributevalue.Number `dynamodbav:"created_at"`
}

func (i InData) Encode() (gallon.Record, error) {
	createdAt, err := i.CreatedAt.Int64()
	if err != nil {
		return nil, err
	}

	return gallon.Record{
		i.Id,
		i.UserId,
		i.GachaType,
		createdAt,
	}, nil
}

type OutData struct {
	Id        string `json:"id"`
	UserId    string `json:"user_id"`
	GachaType string `json:"gacha_type"`
	CreatedAt int64  `json:"created_at"`
}

func Decode(record gallon.Record) (OutData, error) {
	return OutData{
		Id:        record[0].(string),
		UserId:    record[1].(string),
		GachaType: record[2].(string),
		CreatedAt: record[3].(int64),
	}, nil
}

type Worker struct {
	input  inputdynamodb.InputDynamoDbClient
	output outputfile.OutputFileClient
}

func (worker Worker) Run() error {
	pipe := gallon.NewPipe()

	if err := worker.output.Connect(
		context.TODO(),
		pipe,
		func(r gallon.Record) ([]byte, error) {
			outData, err := Decode(r)
			if err != nil {
				return nil, err
			}

			return json.Marshal(&outData)
		},
	); err != nil {
		return err
	}

	if err := worker.input.Connect(
		context.TODO(),
		pipe,
		func(item map[string]types.AttributeValue) (gallon.Record, error) {
			var inData InData
			if err := attributevalue.UnmarshalMap(item, &inData); err != nil {
				return nil, err
			}

			return inData.Encode()
		},
	); err != nil {
		return err
	}

	return nil
}

func start() error {
	input, err := inputdynamodb.NewInputDynamoDbClient(inputdynamodb.InputDynamoDbClientConfig{
		TableName: os.Getenv("TABLE_NAME"),
		Region:    "ap-northeast-1",
		PageLimit: aws.Int(1),
		PageSize:  aws.Int32(10),
	})
	if err != nil {
		return err
	}

	output := outputfile.NewOutputFileClient(outputfile.OutputFileClientConfig{
		FilePath:   "./data/new.jsonl",
		FileFormat: outputfile.Jsonl,
	})

	worker := Worker{
		input:  input,
		output: output,
	}

	if err := worker.Run(); err != nil {
		return err
	}

	return nil
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	log.Logger = log.With().Caller().Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})

	log.Info().Msg("start")
	if err := start(); err != nil {
		log.Error().Stack().Err(err).Msg("failed")
	}
	log.Info().Msg("finished")

	return
}
