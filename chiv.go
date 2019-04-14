// Package chiv archives arbitrarily large relational database tables to Amazon S3.
package chiv

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	// DefaultFormat is CSV.
	DefaultFormatFunc = CSV
	// ErrRecordLength does not match the number of columns.
	ErrRecordLength = errors.New("record length does not match number of columns")
	// ErrParserRegex initialization problem.
	ErrParserRegex = errors.New("initializing parser regex")
)

type archiver struct {
	db        *sql.DB
	s3        *s3manager.Uploader
	format    FormatterFunc
	key       string
	extension string
	null      []byte
}

// NewArchiver constructs an archiver with the given database, S3 uploader and options. Options set on
// creation apply to all calls to Archive unless overridden.
func NewArchiver(db *sql.DB, s3 *s3manager.Uploader, options ...Option) *archiver {
	a := archiver{
		db:     db,
		s3:     s3,
		format: DefaultFormatFunc,
	}

	for _, option := range options {
		option(&a)
	}

	return &a
}

// Archive a database table to S3.
func (a *archiver) Archive(table, bucket string, options ...Option) error {
	return a.ArchiveWithContext(context.Background(), table, bucket, options...)
}

// ArchiveWithContext is like Archive, with context.
func (a *archiver) ArchiveWithContext(ctx context.Context, table, bucket string, options ...Option) error {
	for _, option := range options {
		option(a)
	}

	return a.archive(ctx, table, bucket)
}

func (a *archiver) archive(ctx context.Context, table string, bucket string) error {
	errs := make(chan error)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	go a.download(ctx, w, table, errs)
	go a.upload(ctx, r, table, bucket, errs)

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *archiver) download(ctx context.Context, wc io.WriteCloser, table string, errs chan error) {
	selectAll := fmt.Sprintf(`select * from "%s";`, table)
	rows, err := a.db.QueryContext(ctx, selectAll)
	if err != nil {
		errs <- err
		return
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		errs <- err
		return
	}

	f, err := a.format(wc, columns)
	if err != nil {
		errs <- err
		return
	}

	var (
		rawBytes = make([]sql.RawBytes, len(columns))
		scanned  = make([]interface{}, len(columns))
		record   = make([][]byte, len(columns))
	)
	for i := range rawBytes {
		scanned[i] = &rawBytes[i]
	}

	for rows.Next() {
		err = rows.Scan(scanned...)
		if err != nil {
			errs <- err
			return
		}

		for i, raw := range rawBytes {
			if raw == nil && a.null != nil {
				record[i] = a.null
			} else {
				record[i] = raw
			}
		}

		if err := f.Format(record); err != nil {
			errs <- err
			return
		}
	}

	if err := rows.Err(); err != nil {
		errs <- err
		return
	}

	if err := f.Close(); err != nil {
		errs <- err
		return
	}

	if err := wc.Close(); err != nil {
		errs <- err
		return
	}
}

func (a *archiver) upload(ctx context.Context, r io.Reader, table string, bucket string, errs chan error) {
	if a.key == "" {
		if a.extension != "" {
			a.key = fmt.Sprintf("%s.%s", table, a.extension)
		} else {
			a.key = table
		}
	}

	if _, err := a.s3.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(a.key),
	}); err != nil {
		errs <- err
	}

	errs <- nil
}
