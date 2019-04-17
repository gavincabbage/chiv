// Package chiv_test includes integration tests and benchmarks external to package chiv
// It relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/gavincabbage/chiv"

	_ "github.com/lib/pq"
)

func BenchmarkArchiver_Archive(b *testing.B) {
	var (
		db       = newDB(b, "postgres", os.Getenv("POSTGRES_URL"))
		s3client = newS3Client(b, os.Getenv("AWS_REGION"), os.Getenv("AWS_ENDPOINT"))
		uploader = s3manager.NewUploaderWithClient(s3client)
	)

	const (
		createTable = `CREATE TABLE IF NOT EXISTS "benchmark_table" (t TEXT,i INTEGER,f DECIMAL);`

		insertIntoTable = `INSERT INTO "benchmark_table" VALUES (%s,
				2345,
				
			);`

		dropTable = `
			DROP TABLE "benchmark_table";`
	)

	exec(b, db, createTable)
	defer exec(b, db, dropTable)

	for i := 0; i < 100; i++ {
		exec(b, db, insertIntoTable)
	}

	createBucket(b, s3client, "benchmark_bucket")

	subject := chiv.NewArchiver(db, uploader)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := subject.Archive("benchmark_table", "benchmark_bucket", chiv.WithKey("benchmark_"+string(i))); err != nil {
			b.Error(err)
		}
	}
}
