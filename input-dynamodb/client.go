package inputdynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type InputDynamoDbClientConfig struct {
	TableName string
	Region    string
	PageLimit *int
}

type InputDynamoDbClient struct {
	dynamoDb *dynamodb.Client
	conf     InputDynamoDbClientConfig
	buffer   *bytes.Buffer
	done     int32
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

			encoder := json.NewEncoder(client.buffer)
			for _, item := range output.Items {
				if err := encoder.Encode(item); err != nil {
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
