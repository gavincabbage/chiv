// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gavincabbage/chiv"

	_ "github.com/lib/pq"
)

type test struct {
	name     string
	driver   string
	database string
	setup    string
	teardown string
	options  []chiv.Option
	calls    []call
}

type call struct {
	expected string
	table    string
	bucket   string
	key      string
	options  []chiv.Option
}

func TestArchiver_Archive(t *testing.T) {
	cases := []test{
		{
			name:     "postgres to csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./test/data/postgres.csv",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv key override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options: []chiv.Option{
				chiv.WithKey("postgres_table.csv"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.csv",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv null override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options: []chiv.Option{
				chiv.WithNull("custom_null"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres_with_null.csv",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to json",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
				chiv.WithKey("postgres_table.json"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.json",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to yaml",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
				chiv.WithKey("postgres_table.yaml"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.yaml",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres two formats",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.json",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options: []chiv.Option{
						chiv.WithFormat(chiv.JSON),
						chiv.WithKey("postgres_table.json"),
					},
				},
				{
					expected: "./test/data/postgres.yaml",
					bucket:   "postgres_bucket",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options: []chiv.Option{
						chiv.WithKey("postgres_table.yaml"),
					},
				},
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

			exec(t, db, readFile(t, test.setup))
			defer exec(t, db, readFile(t, test.teardown))

			subject := chiv.NewArchiver(db, uploader, test.options...)
			assert.NotNil(t, subject)

			for _, call := range test.calls {
				createBucket(t, s3client, call.bucket)
				expected := readFile(t, call.expected)

				require.NoError(t, subject.Archive(call.table, call.bucket, call.options...))

				actual := download(t, downloader, call.bucket, call.key)
				require.Equal(t, expected, actual)
			}
		})
	}
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

type errorer interface {
	Error(...interface{})
}
