package inputdynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type InputDynamoDbClientConfig struct {
	TableName string
	Region    string
	PageLimit *int
	PageSize  *int32
}

type InputDynamoDbClient struct {
	dynamoDb *dynamodb.Client
	conf     InputDynamoDbClientConfig
	buffer   *bytes.Buffer
	done     int32
}

func DecodeItem(item map[string]types.AttributeValue) ([]byte, error) {
	var result map[string]interface{}
	if err := attributevalue.UnmarshalMap(item, &result); err != nil {
		return nil, err
	}

	return json.Marshal(&result)
}

func (client *InputDynamoDbClient) Read(p []byte) (int, error) {
	if client.buffer.Len() == 0 {
		if client.done == 1 {
			return 0, io.EOF
		}

		return 0, nil
	}

	return client.buffer.Read(p)
}

func (client *InputDynamoDbClient) Connect(
	ctx context.Context,
) (io.Reader, error) {
	go func() {
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
				log.Fatalf("%v", err)
				return
			}

			for _, item := range output.Items {
				body, err := DecodeItem(item)
				if err != nil {
					log.Fatalf("%v", err)
					return
				}

				if _, err := client.buffer.Write(body); err != nil {
					log.Fatalf("%v", err)
					return
				}
			}

			count++
		}

		atomic.AddInt32(&client.done, 1)
	}()

	return client, nil
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
		buffer:   new(bytes.Buffer),
		done:     0,
	}, nil
}
