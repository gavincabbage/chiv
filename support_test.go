package chiv_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	retryLimit    = 15
	retryInterval = 3 * time.Second
)

type fataler interface {
	Fatal(...interface{})
}

func newDB(f fataler, driver string, url string) *sql.DB {
	db, err := sql.Open(driver, url)
	if err != nil {
		f.Fatal(err)
	}

	for i := 0; i < retryLimit; i++ {
		if err := db.Ping(); err != nil {
			time.Sleep(retryInterval)
			continue
		}
		break
	}

	return db
}

func newS3Client(f fataler, region string, endpoint string) *s3.S3 {
	awsConfig := aws.NewConfig().
		WithRegion(region).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		f.Fatal(err)
	}

	client := s3.New(awsSession)
	client.Endpoint = endpoint

	return client
}

func exec(f fataler, db *sql.DB, statements string) {
	s := strings.Split(statements, ";\n\n")
	for _, statement := range s {
		if _, err := db.Exec(statement); err != nil {
			f.Fatal(err)
		}
	}
}

func createBucket(f fataler, client *s3.S3, name string) {
	if _, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		f.Fatal(err)
	}
}

func deleteBucket(f fataler, client *s3.S3, name string) {
	// we could do this more cleanly with BatchDeleteIterator, but localstack doesn't like batch deletes :shrug:
	out, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		f.Fatal(err)
	}

	for _, o := range out.Contents {
		if _, err := client.DeleteObject(&s3.DeleteObjectInput{
			Key:    o.Key,
			Bucket: aws.String(name),
		}); err != nil {
			f.Fatal(err)
		}
	}

	if _, err := client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		f.Fatal(err)
	}
}

//nolint deadcode false positive
func readFile(f fataler, path string) string {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		f.Fatal(err)
	}

	return string(contents)
}

//nolint deadcode false positive
func download(f fataler, downloader *s3manager.Downloader, bucket string, key string) string {
	b := &aws.WriteAtBuffer{}
	_, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		fmt.Println("error:", bucket, key)
		f.Fatal(err)
	}

	return string(b.Bytes())
}
