package main

import (
	"context"
	"io"
	"log"
	"os"

	inputdynamodb "github.com/myuon/ubiquitous-adventure/input-dynamodb"
	outputfile "github.com/myuon/ubiquitous-adventure/output-file"
)

type Worker struct {
	extractor inputdynamodb.InputDynamoDbClient
	loader    outputfile.OutputFileClient
}

func (worker Worker) Run() error {
	reader, err := worker.extractor.Connect(context.TODO())
	if err != nil {
		return err
	}

	writer, err := worker.loader.Connect(context.TODO())
	if err != nil {
		return err
	}

	total, err := io.Copy(writer, reader)
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
	})
	if err != nil {
		return err
	}

	loader := outputfile.NewOutputFileClient(outputfile.OutputFileClientConfig{
		FilePath:   "./output.json",
		FileFormat: outputfile.Json,
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
