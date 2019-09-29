// +build benchmark

package chiv_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"gavincabbage.com/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func BenchmarkArchiver_Archive(b *testing.B) {
	var (
		benchmarks = []int{1, 10, 100, 1_000, 5_000, 10_000, 1_000_000}
		ctx        = context.Background()
		rows       = &benchmarkRows{
			columnTypes: make([]chiv.Column, 10),
			column:      sql.RawBytes("column_value"),
		}
		uploader = &uploader{}
		bucket   = "benchmark_bucket"
		format   = chiv.WithFormat(format(&benchmarkFormatter{}, nil))
	)

	for _, count := range benchmarks {
		rows.max = count
		b.Run(fmt.Sprintf("benchmark_%d", count), func(bb *testing.B) {
			for j := 0; j < bb.N; j++ {
				if err := chiv.ArchiveRowsWithContext(ctx, rows, uploader, bucket, format); err != nil {
					bb.Error(err)
				}
				rows.ndx = 0
			}
		})
	}
}

type benchmarkRows struct {
	columnTypes []*sql.ColumnType
	column      sql.RawBytes
	ndx, max    int
}

func (r *benchmarkRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return r.columnTypes, nil
}

func (r *benchmarkRows) Next() bool {
	return r.ndx < r.max
}

func (r *benchmarkRows) Scan(c ...interface{}) error {
	for i := range c {
		*(c[i].(*sql.RawBytes)) = r.column
	}

	r.ndx++
	return nil
}

func (r *benchmarkRows) Err() error {
	return nil
}

type benchmarkFormatter struct{}

func (_ *benchmarkFormatter) Format(record [][]byte) error {
	return nil
}
func (_ *benchmarkFormatter) Close() error {
	return nil
}
