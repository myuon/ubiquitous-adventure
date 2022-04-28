package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	inputdynamodb "github.com/myuon/ubiquitous-adventure/input-dynamodb"
	outputfile "github.com/myuon/ubiquitous-adventure/output-file"
)

type Worker struct {
	extractor inputdynamodb.InputDynamoDbClient
	loader    outputfile.OutputFileClient
}

func (worker Worker) Run() error {
	pr, pw := io.Pipe()

	if err := worker.extractor.Connect(context.TODO(), pw); err != nil {
		return err
	}

	writer, err := worker.loader.Connect(context.TODO())
	if err != nil {
		return err
	}

	total, err := io.Copy(writer, pr)
	if err != nil {
		return err
	}

	log.Printf("%v bytes copied", total)

	if err := worker.loader.Close(); err != nil {
		return err
	}

	return nil
}

func start() error {
	extractor, err := inputdynamodb.NewInputDynamoDbClient(inputdynamodb.InputDynamoDbClientConfig{
		TableName: os.Getenv("TABLE_NAME"),
		Region:    "ap-northeast-1",
		PageLimit: aws.Int(1),
		PageSize:  aws.Int32(10),
	})
	if err != nil {
		return err
	}

	loader := outputfile.NewOutputFileClient(outputfile.OutputFileClientConfig{
		FilePath:    "./data/output.jsonl",
		FileFormat:  outputfile.Json,
		Compression: outputfile.Gzip,
	})

	worker := Worker{
		extractor: extractor,
		loader:    loader,
	}

	if err := worker.Run(); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := start(); err != nil {
		log.Fatalf("%v", err)
	}

	return
}
