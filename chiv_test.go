// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"context"
	"os"
	"testing"

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
			setup:    "./test/data/database_setup.sql",
			teardown: "./test/data/database_teardown.sql",
			bucket:   "database_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./test/data/database.csv",
					table:    "database_table",
					key:      "database_table",
					options:  []chiv.Option{},
				},
			},
		},
		{
			name:     "postgres to csv",
			driver:   "postgres",
			database: os.Getenv("POSTGRES_URL"),
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./test/data/postgres.csv",
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
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithKey("postgres_table.csv"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.csv",
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
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithNull("custom_null"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres_with_null.csv",
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
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.JSON),
				chiv.WithKey("postgres_table.json"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.json",
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
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
				chiv.WithKey("postgres_table.yaml"),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.yaml",
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
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.json",
					table:    "postgres_table",
					key:      "postgres_table.json",
					options: []chiv.Option{
						chiv.WithFormat(chiv.JSON),
						chiv.WithKey("postgres_table.json"),
					},
				},
				{
					expected: "./test/data/postgres.yaml",
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
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.YAML),
			},
			calls: []call{
				{
					expected: "./test/data/postgres.yaml",
					table:    "postgres_table",
					key:      "postgres_table.not_yaml",
					options: []chiv.Option{
						chiv.WithExtension("not_yaml"),
					},
				},
				{
					expected: "./test/data/postgres.yaml",
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
			setup:    "./test/data/two_tables_setup.sql",
			teardown: "./test/data/two_tables_teardown.sql",
			bucket:   "postgres_bucket",
			options: []chiv.Option{
				chiv.WithFormat(chiv.CSV),
				chiv.WithExtension("csv"),
			},
			calls: []call{
				{
					expected: "./test/data/two_tables_first.csv",
					table:    "first_table",
					key:      "first_table.csv",
					options:  []chiv.Option{},
				},
				{
					expected: "./test/data/two_tables_second.csv",
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
			setup:    "./test/data/postgres_setup.sql",
			teardown: "./test/data/postgres_teardown.sql",
			bucket:   "postgres_bucket",
			options:  []chiv.Option{},
			calls: []call{
				{
					expected: "./test/data/postgres_subset.csv",
					table:    "postgres_table",
					key:      "postgres_table",
					options: []chiv.Option{
						chiv.WithColumns("id", "text_column", "int_column"),
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
		setup      = "./test/data/postgres_setup.sql"
		teardown   = "./test/data/postgres_teardown.sql"
		expected   = "./test/data/postgres.csv"
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
