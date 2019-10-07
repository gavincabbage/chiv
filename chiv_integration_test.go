// +build integration

package chiv_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/require"

	"gavincabbage.com/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func TestArchiver_Archive(t *testing.T) {
	type call struct {
		expected string
		table    string
		key      string
		options  []chiv.Option
	}

	cases := []struct {
		name     string
		driver   string
		database string
		setup    string
		teardown string
		bucket   string
		options  []chiv.Option
		calls    []call
	}{
		{
			name:     "postgres to csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv key override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithKey("archive.csv"),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.csv",
					table:    "postgres_table",
					key:      "archive.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv null override",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithNull("custom_null"),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres_with_null.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to json",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.json",
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
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.yaml",
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
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.json",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options: []chiv.Option{
						chiv.WithFormat(chiv.JSON),
					},
				},
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres one-off extension",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.not_yaml",
					options: []chiv.Option{
						chiv.WithExtension("not_yaml"),
					},
				},
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres two tables",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/two_tables_setup.sql",
			teardown: "./testdata/postgres/two_tables_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.CSV),
			},
			calls: []call{
				{
					expected: "./testdata/postgres/two_tables_first.csv",
					table:    "first_table",
					key:      "first_table.csv",
					options:  []chiv.Option{},
				},
				{
					expected: "./testdata/postgres/two_tables_second.csv",
					table:    "second_table",
					key:      "second_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "with columns",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres_subset.csv",
					table:    "postgres_table",
					key:      "postgres_table.csv",
					options: []chiv.Option{
						chiv.WithColumns("id", "text_column", "int_column"),
					},
				},
			},
		},
		{
			name:     "mariadb happy path csv",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "test_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.csv",
					table:    "test_table",
					key:      "test_table.csv",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path yaml",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.yaml",
					table:    "test_table",
					key:      "test_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path json",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
			},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.json",
					table:    "test_table",
					key:      "test_table.json",
					options:  []chiv.Option{},
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
			defer db.Close()

			exec(t, db, readFile(t, test.setup))
			defer exec(t, db, readFile(t, test.teardown))

			createBucket(t, s3client, test.bucket)
			defer deleteBucket(t, s3client, test.bucket)

			subject := chiv.NewArchiver(db, uploader, test.options...)
			require.NotNil(t, subject)

			for _, call := range test.calls {
				require.NoError(t, subject.Archive(call.table, test.bucket, call.options...))

				expected := readFile(t, call.expected)
				actual := download(t, downloader, test.bucket, call.key)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestArchiveWithContext(t *testing.T) {
	var (
		database   = os.Getenv("POSTGRES_URL")
		driver     = "postgres"
		bucket     = "postgres_bucket"
		table      = "postgres_table"
		key        = "postgres_table.csv"
		setup      = "./testdata/postgres/postgres_setup.sql"
		teardown   = "./testdata/postgres/postgres_teardown.sql"
		expected   = "./testdata/postgres/postgres.csv"
		db         = newDB(t, driver, database)
		s3client   = newS3Client(t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader   = s3manager.NewUploaderWithClient(s3client)
		downloader = s3manager.NewDownloaderWithClient(s3client)
	)
	defer db.Close()

	exec(t, db, readFile(t, setup))
	defer exec(t, db, readFile(t, teardown))

	createBucket(t, s3client, bucket)
	defer deleteBucket(t, s3client, bucket)

	require.NoError(t, chiv.ArchiveWithContext(context.Background(), db, uploader, table, bucket))

	actual := download(t, downloader, bucket, key)
	require.Equal(t, readFile(t, expected), actual)
}

const (
	retryLimit    = 15
	retryInterval = 3 * time.Second
)

func newDB(t testing.TB, driver string, url string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, url)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < retryLimit; i++ {
		if err := db.Ping(); err != nil {
			if i < retryLimit {
				time.Sleep(retryInterval)
				continue
			}
			t.Fatalf("connecting to %s: %s", driver, err)
		}
		break
	}

	return db
}

func newS3Client(t testing.TB, region string, endpoint string) *s3.S3 {
	t.Helper()

	awsConfig := aws.NewConfig().
		WithRegion(region).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		t.Fatal(err)
	}

	client := s3.New(awsSession)
	client.Endpoint = endpoint

	return client
}

func exec(t testing.TB, db *sql.DB, statements string) {
	s := strings.Split(statements, ";\n\n")
	for _, statement := range s {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
}

func createBucket(t testing.TB, client *s3.S3, name string) {
	t.Helper()

	if _, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		t.Fatal(err)
	}
}

func deleteBucket(t testing.TB, client *s3.S3, name string) {
	t.Helper()

	// we could do this more cleanly with BatchDeleteIterator, but localstack doesn't like batch deletes :shrug:
	out, err := client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, o := range out.Contents {
		if _, err := client.DeleteObject(&s3.DeleteObjectInput{
			Key:    o.Key,
			Bucket: aws.String(name),
		}); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	}); err != nil {
		t.Fatal(err)
	}
}

func readFile(t testing.TB, path string) string {
	t.Helper()

	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(contents)
}

func download(t testing.TB, downloader *s3manager.Downloader, bucket string, key string) string {
	t.Helper()

	b := &aws.WriteAtBuffer{}
	_, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		fmt.Println("error:", bucket, key)
		t.Fatal(err)
	}

	return string(b.Bytes())
}
