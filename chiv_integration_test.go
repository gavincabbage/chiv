// +build integration

// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external databases and s3 (localstack) via docker-compose.
package chiv_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gavincabbage.com/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type test struct {
	name     string
	driver   string
	database string
	setup    string
	teardown string
	bucket   string
	options  []chiv.Option
	calls    []call
}

type call struct {
	expected string
	table    string
	key      string
	options  []chiv.Option
}

func TestArchiver_Archive(t *testing.T) {
	cases := []test{
		{
			name:     "happy path csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./testdata/postgres/postgres_setup.sql",
			teardown: "./testdata/postgres/postgres_teardown.sql",
			bucket:   "test_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./testdata/postgres/postgres.csv",
					table:    "postgres_table",
					key:      "postgres_table",
					options:  []chiv.Option{},
				},
			},
		},
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
					key:      "postgres_table",
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
				chiv.WithKey("postgres_table.csv"),
			},
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
					key:      "postgres_table",
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
				chiv.WithKey("postgres_table.json"),
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
				chiv.WithKey("postgres_table.yaml"),
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
						chiv.WithKey("postgres_table.json"),
					},
				},
				{
					expected: "./testdata/postgres/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.yaml",
					options: []chiv.Option{
						chiv.WithKey("postgres_table.yaml"),
					},
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
					key:      "postgres_table",
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
				chiv.WithExtension("csv"),
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
					key:      "postgres_table",
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
					key:      "test_table",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path csv",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
				chiv.WithKey("mariadb_table.yaml"),
			},
			calls: []call{
				{
					expected: "./testdata/mariadb/happy.yaml",
					table:    "test_table",
					key:      "mariadb_table.yaml",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "mariadb happy path csv",
			driver:   "mysql",
			database: os.Getenv("MARIADB_URL"),
			setup:    "./testdata/mariadb/setup.sql",
			teardown: "./testdata/mariadb/teardown.sql",
			bucket:   "mariadb_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
				chiv.WithKey("test_table.json"),
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

			exec(t, db, readFile(t, test.setup))
			defer exec(t, db, readFile(t, test.teardown))

			createBucket(t, s3client, test.bucket)
			defer deleteBucket(t, s3client, test.bucket)

			subject := chiv.NewArchiver(db, uploader, test.options...)
			assert.NotNil(t, subject)

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
		setup      = "./testdata/postgres/postgres_setup.sql"
		teardown   = "./testdata/postgres/postgres_teardown.sql"
		expected   = "./testdata/postgres/postgres.csv"
		db         = newDB(t, driver, database)
		s3client   = newS3Client(t, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader   = s3manager.NewUploaderWithClient(s3client)
		downloader = s3manager.NewDownloaderWithClient(s3client)
	)

	exec(t, db, readFile(t, setup))
	defer exec(t, db, readFile(t, teardown))

	createBucket(t, s3client, bucket)
	defer deleteBucket(t, s3client, bucket)

	require.NoError(t, chiv.ArchiveWithContext(context.Background(), db, uploader, table, bucket))

	actual := download(t, downloader, bucket, table)
	require.Equal(t, readFile(t, expected), actual)
}
