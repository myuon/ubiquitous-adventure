package inputdynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type InputDynamoDbClientConfig struct {
	TableName string
	Region    string
}

type InputDynamoDbClient struct {
	dynamoDb *dynamodb.Client
	conf     InputDynamoDbClientConfig
}

func (client *InputDynamoDbClient) Connect(
	ctx context.Context,
) (io.Reader, error) {
	output, err := client.dynamoDb.Scan(ctx, &dynamodb.ScanInput{
		TableName: &client.conf.TableName,
	})
	if err != nil {
		return nil, err
	}

	out, err := json.Marshal(&output.Items)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(out), nil
}

func NewInputDynamoDbClient(conf InputDynamoDbClientConfig) (InputDynamoDbClient, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(conf.Region))
	if err != nil {
		return InputDynamoDbClient{}, err
	}

	client := dynamodb.NewFromConfig(cfg)

	return InputDynamoDbClient{
		dynamoDb: client,
		conf:     conf,
	}, nil
}
