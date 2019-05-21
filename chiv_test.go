// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
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
		//{
		//	name:     "mysql to csv",
		//	driver:   "mysql",
		//	database: os.Getenv("MYSQL_URL"),
		//	setup:    "./test/data/mysql_setup.sql",
		//	teardown: "./test/data/mysql_teardown.sql",
		//	bucket:   "mysql_bucket",
		//	options:  []chiv.Option{},
		//	calls: []call{
		//		{
		//			expected: "./test/data/mysql.csv",
		//			table:    "mysql_table",
		//			key:      "mysql_table",
		//			options:  []chiv.Option{},
		//		},
		//	},
		//},
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
