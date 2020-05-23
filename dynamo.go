package dynamods

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

const (
	// listMax is the largest amount of objects you can request from S3 in a list
	// call.
	listMax = 1000

	// deleteMax is the largest amount of objects you can delete from S3 in a
	// delete objects call.
	deleteMax = 1000

	defaultWorkers = 100
	keyKey         = "k"
	valueKey       = "v"
	ttlKey         = "expires"
)

type DynamoTable struct {
	Config
	DynamoDB *dynamodb.DynamoDB
}

type Config struct {
	AccessKey      string
	SecretKey      string
	SessionToken   string
	TableName      string
	Region         string
	RegionEndpoint string
	RootDirectory  string
	Workers        int
}

func NewDynamoDatastore(conf Config) (*DynamoTable, error) {
	if conf.Workers == 0 {
		conf.Workers = defaultWorkers
	}

	awsConfig := aws.NewConfig()
	sess, err := session.NewSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create new session: %s", err)
	}

	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.StaticProvider{Value: credentials.Value{
			AccessKeyID:     conf.AccessKey,
			SecretAccessKey: conf.SecretKey,
			SessionToken:    conf.SessionToken,
		}},
		&credentials.EnvProvider{},
		&credentials.SharedCredentialsProvider{},
		&ec2rolecreds.EC2RoleProvider{Client: ec2metadata.New(sess)},
	})

	if conf.RegionEndpoint != "" {
		awsConfig.WithS3ForcePathStyle(true)
		awsConfig.WithEndpoint(conf.RegionEndpoint)
	}

	awsConfig.WithCredentials(creds)
	awsConfig.WithRegion(conf.Region)

	sess, err = session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create new session with aws config: %s", err)
	}
	dynamoObj := dynamodb.New(sess)

	return &DynamoTable{
		DynamoDB: dynamoObj,
		Config:   conf,
	}, nil
}

func (s *DynamoTable) Put(k ds.Key, value []byte) error {
	return s.put(k, value, -1)
}

func (s *DynamoTable) put(k ds.Key, value []byte, ttl int64) error {
	item := map[string]*dynamodb.AttributeValue{
		keyKey:   {S: aws.String(k.String())},
		valueKey: {B: value},
	}
	if ttl > 0 {
		item[ttlKey] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(ttl, 10))}
	}
	_, err := s.DynamoDB.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(s.TableName),
		Item:      item,
	})
	return parseError(err)
}

func (s *DynamoTable) Get(k ds.Key) ([]byte, error) {
	resp, err := s.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key:       dynamoKeyFromDsKey(k.String()),
		TableName: aws.String(s.TableName),
	})
	if err != nil {
		return nil, parseError(err)
	}

	if resp.Item[valueKey] != nil {
		return resp.Item[valueKey].B, nil
	}

	return nil, ds.ErrNotFound
}

func (s *DynamoTable) PutWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	ttlInt := time.Now().UTC().Add(ttl).Unix()
	return s.put(key, value, ttlInt)
}

func (s *DynamoTable) SetTTL(key ds.Key, ttl time.Duration) error {
	ttlInt := time.Now().UTC().Add(ttl).Unix()
	_, err := s.DynamoDB.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:        aws.String(s.TableName),
		Key:              dynamoKeyFromDsKey(key.String()),
		UpdateExpression: aws.String("SET " + ttlKey + "= :ttlInt"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ttlInt": {N: aws.String(strconv.FormatInt(ttlInt, 10))},
		},
	})
	return parseError(err)
}

func (s *DynamoTable) GetExpiration(k ds.Key) (time.Time, error) {
	resp, err := s.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key:                  dynamoKeyFromDsKey(k.String()),
		ProjectionExpression: aws.String(ttlKey),
		TableName:            aws.String(s.TableName),
		ConsistentRead:       aws.Bool(true),
	})

	err = parseError(err)

	if err != nil {
		return time.Unix(0, 0), parseError(err)
	}

	if resp.Item[ttlKey] != nil {
		int, err := strconv.ParseInt(*resp.Item[ttlKey].N, 10, 64)
		if err != nil {
			return time.Unix(0, 0), err
		}
		return time.Unix(int, 0), nil
	}

	return time.Unix(0, 0), nil
}

func (s *DynamoTable) Has(k ds.Key) (exists bool, err error) {
	resp, err := s.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key:                  dynamoKeyFromDsKey(k.String()),
		ProjectionExpression: aws.String(keyKey),
		TableName:            aws.String(s.TableName),
		ConsistentRead:       aws.Bool(true),
	})

	err = parseError(err)

	if err != nil {
		return false, parseError(err)
	}

	if resp.Item[keyKey] != nil {
		return true, nil
	}

	return false, nil
}

func (s *DynamoTable) GetSize(k ds.Key) (size int, err error) {
	bits, err := s.Get(k)
	if err != nil {
		return -1, parseError(err)
	}
	return len(bits), nil
}

func (s *DynamoTable) Delete(k ds.Key) error {
	_, err := s.DynamoDB.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(s.TableName),
		Key:       dynamoKeyFromDsKey(k.String()),
	})
	return parseError(err)
}

func (s *DynamoTable) Sync(prefix ds.Key) error {
	return nil
}

func (s *DynamoTable) Query(q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("dynamo: filters or orders are not supported")
	}

	limit := q.Limit + q.Offset
	if limit == 0 || limit > listMax {
		limit = listMax
	}

	var projectionExpression *string
	if q.KeysOnly {
		projectionExpression = aws.String(keyKey)
	}

	var filterExpression *string
	if q.Prefix != "" {
		filterExpression = aws.String("begins_with(" + keyKey + "," + q.Prefix + ")")
	}

	resp, err := s.DynamoDB.Scan(&dynamodb.ScanInput{
		FilterExpression:     filterExpression,
		ProjectionExpression: projectionExpression,
		Limit:                aws.Int64(int64(limit)),
		TableName:            aws.String(s.TableName),
	})

	if err != nil {
		return nil, err
	}

	index := q.Offset
	nextValue := func() (dsq.Result, bool) {
		for int64(index) >= *resp.Count {
			if len(resp.LastEvaluatedKey) == 0 {
				return dsq.Result{}, false
			}

			index -= len(resp.Items)

			resp, err = s.DynamoDB.Scan(&dynamodb.ScanInput{
				FilterExpression:     aws.String("begins_with(" + keyKey + "," + q.Prefix + ")"),
				ProjectionExpression: projectionExpression,
				Limit:                aws.Int64(listMax),
				TableName:            aws.String(s.TableName),
				ExclusiveStartKey:    resp.LastEvaluatedKey,
			})
			if err != nil {
				return dsq.Result{Error: err}, false
			}
		}

		entry := dsq.Entry{
			Key: ds.NewKey(*resp.Items[index][keyKey].S).String(),
		}
		if !q.KeysOnly {
			value := resp.Items[index][valueKey].B
			entry.Value = value
		}

		index++
		return dsq.Result{Entry: entry}, true
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}

func (s *DynamoTable) Batch() (ds.Batch, error) {
	return &s3Batch{
		s:          s,
		ops:        make(map[string]batchOp),
		numWorkers: s.Workers,
	}, nil
}

func (s *DynamoTable) Close() error {
	return nil
}

func parseError(err error) error {
	if dynErr, ok := err.(awserr.Error); ok && dynErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
		return ds.ErrNotFound
	}
	return err
}

type s3Batch struct {
	s          *DynamoTable
	ops        map[string]batchOp
	numWorkers int
}

type batchOp struct {
	val    []byte
	delete bool
}

func (b *s3Batch) Put(k ds.Key, val []byte) error {
	b.ops[k.String()] = batchOp{
		val:    val,
		delete: false,
	}
	return nil
}

func (b *s3Batch) Delete(k ds.Key) error {
	b.ops[k.String()] = batchOp{
		val:    nil,
		delete: true,
	}
	return nil
}

func dynamoKeyFromDsKey(k string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		keyKey: {S: aws.String(k)},
	}
}

func (b *s3Batch) Commit() error {
	var (
		deleteObjs []*dynamodb.WriteRequest
		putKeys    []ds.Key
	)
	for k, op := range b.ops {
		if op.delete {
			deleteObjs = append(deleteObjs, &dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: dynamoKeyFromDsKey(k),
				},
			})
		} else {
			putKeys = append(putKeys, ds.NewKey(k))
		}
	}

	numJobs := len(putKeys) + (len(deleteObjs) / deleteMax)
	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := b.numWorkers
	if numJobs < numWorkers {
		numWorkers = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	defer wg.Wait()

	for w := 0; w < numWorkers; w++ {
		go func() {
			defer wg.Done()
			worker(jobs, results)
		}()
	}

	for _, k := range putKeys {
		jobs <- b.newPutJob(k, b.ops[k.String()].val)
	}

	if len(deleteObjs) > 0 {
		for i := 0; i < len(deleteObjs); i += deleteMax {
			limit := deleteMax
			if len(deleteObjs[i:]) < limit {
				limit = len(deleteObjs[i:])
			}

			jobs <- b.newDeleteJob(deleteObjs[i : i+limit])
		}
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		err := <-results
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("s3ds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

func (b *s3Batch) newPutJob(k ds.Key, value []byte) func() error {
	return func() error {
		return b.s.Put(k, value)
	}
}

func (b *s3Batch) newDeleteJob(objs []*dynamodb.WriteRequest) func() error {
	return func() error {
		resp, err := b.s.DynamoDB.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				b.s.TableName: objs,
			},
		})

		if err != nil {
			return err
		}

		if len(resp.UnprocessedItems[b.s.TableName]) > 0 {
			return fmt.Errorf("failed to delete objects: %s", resp.UnprocessedItems[b.s.TableName])
		}

		return nil
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}

var _ ds.Batching = (*DynamoTable)(nil)

var _ ds.TTLDatastore = (*DynamoTable)(nil)
