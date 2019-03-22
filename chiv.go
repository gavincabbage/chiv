// Package chiv archives arbitrarily large relational database tables to Amazon S3.
package chiv

import (
	"context"
	"database/sql"
	"io"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	format Format
}

const (
	DefaultFormat = CSV
)

// NewArchiver constructs an Archiver with the given database connection, S3 uploader and options.
func NewArchiver(db *sql.DB, s3 *s3manager.Uploader, options ...Option) *Archiver {
	a := Archiver{
		db:     db,
		s3:     s3,
		format: DefaultFormat,
	}

	for _, option := range options {
		option(&a)
	}

	return &a
}

// Archive a database table to S3.
func (a *Archiver) Archive(table string, bucket string, options ...Option) error {
	return a.ArchiveWithContext(context.Background(), table, bucket, options...)
}

// Archive a database table to S3 with context.
func (a *Archiver) ArchiveWithContext(ctx context.Context, table string, bucket string, options ...Option) error {
	archiver := archiver{
		db:     a.db,
		s3:     a.s3,
		ctx:    ctx,
		config: a,
	}

	for _, option := range options {
		option(archiver.config)
	}

	return archiver.archive(table)
}

type archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	ctx    context.Context
	config *Archiver
}

func (a *archiver) archive(table string) error {
	const selectAll = "SELECT * FROM $1"

	rows, err := a.db.QueryContext(a.ctx, selectAll, table)
	if err != nil {
		return err
	}
	defer rows.Close()

	r, w := io.Pipe() // TODO figuring this all out...

	for rows.Next() {

	}

	if err := rows.Err(); err != nil {
		return err
	}

	// TODO the work
	// 		db cursor selecting: ???
	// 		s3 streaming: https://docs.aws.amazon.com/code-samples/latest/catalog/go-s3-upload_arbitrary_sized_stream.go.html

	return nil // TODO return size or some other info along w/ error?
}
