package chiv_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type errorer interface {
	Error(...interface{})
}

func newDB(e errorer, driver string, url string) *sql.DB {
	db, err := sql.Open(driver, url)
	if err != nil {
		e.Error(err)
	}

	return db
}

func newS3Client(e errorer, region string, endpoint string) *s3.S3 {
	awsConfig := aws.NewConfig().
		WithRegion(region).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		e.Error(err)
	}

	client := s3.New(awsSession)
	client.Endpoint = endpoint

	return client
}

func exec(e errorer, db *sql.DB, statements string) {
	s := strings.Split(statements, ";\n\n")
	for _, statement := range s {
		if _, err := db.Exec(statement); err != nil {
			fmt.Println(statement)
			e.Error(err)
		}
	}
}

func createBucket(e errorer, client *s3.S3, name string) {
	if _, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		e.Error(err)
	}
}

func deleteBucket(e errorer, client *s3.S3, name string) {
	// we could do this more cleanly with BatchDeleteIterator, but localstack doesn't like batch deletes :shrug:
	out, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		e.Error(err)
	}

	for _, o := range out.Contents {
		if _, err := client.DeleteObject(&s3.DeleteObjectInput{
			Key:    o.Key,
			Bucket: aws.String(name),
		}); err != nil {
			e.Error(err)
		}
	}

	if _, err := client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		e.Error(err)
	}
}

func readFile(e errorer, path string) string {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		e.Error(err)
	}

	return string(contents)
}

func download(e errorer, downloader *s3manager.Downloader, bucket string, key string) string {
	b := &aws.WriteAtBuffer{}
	_, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		e.Error(err)
	}

	return string(b.Bytes())
}
