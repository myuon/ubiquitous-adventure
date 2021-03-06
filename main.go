package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/myuon/ubiquitous-adventure/gallon"
	inputfile "github.com/myuon/ubiquitous-adventure/input-file"
	outputfile "github.com/myuon/ubiquitous-adventure/output-file"
	"github.com/myuon/ubiquitous-adventure/testdata"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

type InDataJson struct {
	Id        string `json:"id"`
	UserId    string `json:"user_id"`
	GachaType string `json:"gacha_type"`
	CreatedAt int64  `json:"created_at"`
}

func (i InDataJson) Encode() (gallon.Record, error) {
	return gallon.Record{
		i.Id,
		i.UserId,
		i.GachaType,
		i.CreatedAt,
	}, nil
}

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

func start() error {
	/*
		input, err := inputdynamodb.NewInputDynamoDbClient(inputdynamodb.InputDynamoDbClientConfig{
			TableName: os.Getenv("TABLE_NAME"),
			Region:    "ap-northeast-1",
			PageLimit: aws.Int(1),
			PageSize:  aws.Int32(10),
			Decoder: func(item map[string]types.AttributeValue) (gallon.Record, error) {
				var inData InData
				if err := attributevalue.UnmarshalMap(item, &inData); err != nil {
					return nil, err
				}

				return inData.Encode()
			},
		})
		if err != nil {
			return err
		}
	*/
	input := inputfile.NewInputFileClient(inputfile.InputFileClientConfig{
		FilePath:   "./data/output.jsonl",
		FileFormat: inputfile.Jsonl,
		Decoder: func(b []byte) (gallon.Record, error) {
			var inData InDataJson
			if err := json.Unmarshal(b, &inData); err != nil {
				return nil, err
			}

			return inData.Encode()
		},
	})

	output := outputfile.NewOutputFileClient(outputfile.OutputFileClientConfig{
		FilePath:   "./data/new.jsonl",
		FileFormat: outputfile.Jsonl,
		Encoder: func(r gallon.Record) ([]byte, error) {
			outData, err := Decode(r)
			if err != nil {
				return nil, err
			}

			return json.Marshal(&outData)
		},
	})

	g := gallon.NewGallon(
		input,
		output,
	)

	if err := g.Run(); err != nil {
		return err
	}

	return nil
}

func mainGallon() {
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

func mainGenData() {
	if err := testdata.CreateFile(fmt.Sprintf("users-%v.jsonl", gofakeit.LetterN(10)), testdata.GenerateFakeUsers(10000).Jsonl()); err != nil {
		panic(err)
	}
}

func main() {
	// mainGenData
	mainGallon()
}
