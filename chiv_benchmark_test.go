// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/gavincabbage/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func BenchmarkArchiver_Archive(b *testing.B) {
	const (
		bucket = "benchmark_bucket"
		table  = "benchmark_table"

		createTable     = "CREATE TABLE IF NOT EXISTS benchmark_table (s_col TEXT, d_col INTEGER, f_col DECIMAL);"
		insertIntoTable = "INSERT INTO benchmark_table VALUES ('%s', %d, %f);"
		dropTable       = "DROP TABLE benchmark_table;"

		charset    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		textLength = 1000
	)

	var (
		benchmarks = []int{1, 10, 100, 1000, 3000}

		db       = newDB(b, "postgres", os.Getenv("POSTGRES_URL"))
		s3client = newS3Client(b, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader = s3manager.NewUploaderWithClient(s3client)
		random   = rand.New(rand.NewSource(time.Now().Unix()))
	)

	for _, count := range benchmarks {
		exec(b, db, createTable)
		createBucket(b, s3client, bucket)

		for i := 0; i < count; i++ {
			statement := fmt.Sprintf(insertIntoTable, text(random, charset, textLength), i, 42.42)
			exec(b, db, statement)
		}

		b.Run(fmt.Sprintf("benchmark_%d", count), func(*testing.B) {
			for j := 0; j < b.N; j++ {
				key := fmt.Sprintf("benchmark_%d_%d", count, j)
				if err := chiv.Archive(db, uploader, table, bucket, chiv.WithKey(key)); err != nil {
					b.Error(err)
				}
			}
		})

		exec(b, db, dropTable)
		deleteBucket(b, s3client, bucket)
	}

}

func text(r *rand.Rand, charset string, length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}

	return string(b)
}
