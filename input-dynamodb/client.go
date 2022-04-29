package inputdynamodb

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/myuon/ubiquitous-adventure/gallon"
	"github.com/rs/zerolog/log"
)

type InputDynamoDbClientConfig struct {
	TableName string
	Region    string
	PageLimit *int
	PageSize  *int32
	Decoder   func(item map[string]types.AttributeValue) (gallon.Record, error)
}

type InputDynamoDbClient struct {
	dynamoDb *dynamodb.Client
	conf     InputDynamoDbClientConfig
}

func DecodeItem(item map[string]types.AttributeValue) ([]byte, error) {
	var result map[string]interface{}
	if err := attributevalue.UnmarshalMap(item, &result); err != nil {
		return nil, err
	}

	return json.Marshal(&result)
}

func (client *InputDynamoDbClient) Connect(
	ctx context.Context,
	writer gallon.WriteCloser,
) error {
	pager := dynamodb.NewScanPaginator(client.dynamoDb, &dynamodb.ScanInput{
		TableName: &client.conf.TableName,
		Limit:     client.conf.PageSize,
	})
	count := 1

	for pager.HasMorePages() {
		if client.conf.PageLimit != nil && count > *client.conf.PageLimit {
			break
		}

		output, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}

		for _, item := range output.Items {
			r, err := client.conf.Decoder(item)
			if err != nil {
				log.Error().Err(err).Msg("failed to decode item")
				continue
			}

			if err := writer.Write(r); err != nil {
				log.Error().Err(err).Msg("failed to write record")
				continue
			}
		}

		count++
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
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
