package s3ds

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestSuite(t *testing.T) {
	// run docker-compose up in this repo in order to get a local
	// s3 running on port 4572
	config := Config{
		RegionEndpoint: "http://localhost:4569",
		TableName:      "localTable",
		Region:         "us-east-1",
		AccessKey:      "localonlyac",
		SecretKey:      "localonlysk",
	}

	dynds, err := NewDynamoDatastore(config)
	if err != nil {
		t.Error(err)
	}

	err = devMakeTable(dynds.DynamoDB, config.TableName)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, dynds)
	})
	t.Run("not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, dynds)
	})
	t.Run("many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, dynds)
	})
}

func devMakeTable(dynds *dynamodb.DynamoDB, tableName string) error {
	_, err := dynds.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String(keyKey), AttributeType: aws.String("S")},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{KeyType: aws.String("HASH"), AttributeName: aws.String(keyKey)},
		},
		TableName: aws.String(tableName),
	})
	if err == nil {
		time.Sleep(2 * time.Second) // give it time to go
	}
	return err
}
