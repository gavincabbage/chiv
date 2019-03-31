// Package chiv_test includes integration tests external to package chiv
// and relies on external services postgres and s3 (localstack) via CodeShip.
package pkg_test

import (
	"database/sql"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gavincabbage/pkg/chiv"
)

func TestArchiver_Archive(t *testing.T) {
	cases := []struct {
		name     string
		driver   string
		database string
		setup    string
		teardown string
		expected string
		bucket   string
		table    string
		key      string
		options  []chiv.Option
	}{
		{
			name:     "postgres to csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres_setup.sql",
			teardown: "./testdata/postgres_teardown.sql",
			expected: "./testdata/postgres.csv",
			bucket:   "postgres_bucket",
			table:    "postgres_table",
			key:      "postgres_table.csv",
			options:  []chiv.Option{},
		},
		{
			name:     "postgres to csv key override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres_setup.sql",
			teardown: "./testdata/postgres_teardown.sql",
			expected: "./testdata/postgres.csv",
			bucket:   "postgres_bucket",
			table:    "postgres_table",
			key:      "postgres_custom_key",
			options: []chiv.Option{
				chiv.WithKey("postgres_custom_key"),
			},
		},
		{
			name:     "postgres to csv null override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres_setup.sql",
			teardown: "./testdata/postgres_teardown.sql",
			expected: "./testdata/postgres_with_null.csv",
			bucket:   "postgres_bucket",
			table:    "postgres_table",
			key:      "postgres_table.csv",
			options: []chiv.Option{
				chiv.WithNull("custom_null"),
			},
		},
		{
			name:     "postgres to json",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres_setup.sql",
			teardown: "./testdata/postgres_teardown.sql",
			expected: "./testdata/postgres.json",
			bucket:   "postgres_bucket",
			table:    "postgres_table",
			key:      "postgres_table.json",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var (
				db         = newDB(t, test.driver, test.database)
				s3client   = newS3Client(t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
				uploader   = s3manager.NewUploaderWithClient(s3client)
				downloader = s3manager.NewDownloaderWithClient(s3client)
			)

			exec(t, db, test.setup)
			defer exec(t, db, test.teardown)

			createBucket(t, s3client, test.bucket)
			expected := readFile(t, test.expected)

			subject := chiv.NewArchiver(db, uploader)
			assert.NotNil(t, subject)

			require.NoError(t, subject.Archive(test.table, test.bucket, test.options...))

			actual := download(t, downloader, test.bucket, test.key)
			require.Equal(t, expected, actual)
		})
	}
}

func newDB(t *testing.T, driver string, url string) *sql.DB {
	db, err := sql.Open(driver, url)
	if err != nil {
		t.Error(err)
	}

	return db
}

func newS3Client(t *testing.T, region string, endpoint string) *s3.S3 {
	awsConfig := aws.NewConfig().
		WithRegion(region).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		t.Error(err)
	}

	client := s3.New(awsSession)
	client.Endpoint = endpoint

	return client
}

func exec(t *testing.T, db *sql.DB, path string) {
	file := readFile(t, path)
	statements := strings.Split(string(file), ";\n")
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Error(err)
		}
	}
}

func createBucket(t *testing.T, client *s3.S3, name string) {
	if _, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		t.Error(err)
	}
}

func readFile(t *testing.T, path string) string {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Error(err)
	}

	return string(contents)
}

func download(t *testing.T, downloader *s3manager.Downloader, bucket string, key string) string {
	b := &aws.WriteAtBuffer{}
	_, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Error(err)
	}

	return string(b.Bytes())
}
