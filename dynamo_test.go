package dynamods

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestSuite(t *testing.T) {
	// run docker-compose up in this repo in order to get a local
	// s3 running on port 4572
	config := Config{
		RegionEndpoint: "http://localhost:4566",
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
	t.Run("ttl works", func(t *testing.T) {
		k := ds.NewKey("/test/key")
		dur := 10 * time.Second
		expires := time.Now().UTC().Add(dur)
		err := dynds.PutWithTTL(k, []byte("hi"), dur)
		if err != nil {
			t.Fatal(err)
		}

		ttl, err := dynds.GetExpiration(k)
		if err != nil {
			t.Fatal(err)
		}
		if ttl.Unix() != expires.Unix() {
			t.Errorf("%v != %v", ttl.Unix(), expires.Unix())
		}

		err = dynds.SetTTL(k, dur)
		if err != nil {
			t.Fatal(err)
		}
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

	_, err = dynds.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			AttributeName: aws.String(ttlKey),
			Enabled:       aws.Bool(true),
		},
	})
	if err == nil {
		time.Sleep(2 * time.Second) // give it time to go
	}
	return err
}
