// Package chiv archives relational database tables to Amazon S3.
package chiv

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/errgroup"
)

// Archive a database table to S3.
func Archive(db database, s3 uploader, table, bucket string, options ...Option) error {
	return ArchiveWithContext(context.Background(), db, s3, table, bucket, options...)
}

// ArchiveWithContext is like Archive, with context.
func ArchiveWithContext(ctx context.Context, db database, s3 uploader, table, bucket string, options ...Option) error {
	return NewArchiver(db, s3).ArchiveWithContext(ctx, table, bucket, options...)
}

// ArchiveRows to S3.
func ArchiveRows(rows rows, s3 uploader, bucket string, options ...Option) error {
	return ArchiveRowsWithContext(context.Background(), rows, s3, bucket, options...)
}

// ArchiveRowsWithContext is like ArchiveRows, with context.
func ArchiveRowsWithContext(ctx context.Context, rows rows, s3 uploader, bucket string, options ...Option) error {
	return NewArchiver(nil, s3).ArchiveRowsWithContext(ctx, rows, bucket, options...)
}

// Archiver archives database tables to Amazon S3.
type Archiver struct {
	db        database
	s3        uploader
	format    FormatterFunc
	key       string
	extension string
	null      []byte
	columns   []string
}

// NewArchiver constructs an archiver with the given database, S3 uploader and options.
// Options set on creation apply to all calls to Archive unless overridden.
func NewArchiver(db database, s3 uploader, options ...Option) *Archiver {
	a := Archiver{
		db:     db,
		s3:     s3,
		format: CSV,
	}

	for _, option := range options {
		option(&a)
	}

	return &a
}

// Archive a database table to S3. Any options provided override those set on creation.
func (a *Archiver) Archive(table, bucket string, options ...Option) error {
	return a.ArchiveWithContext(context.Background(), table, bucket, options...)
}

// ArchiveWithContext is like Archive, with context. Any options provided override those set on creation.
func (a *Archiver) ArchiveWithContext(ctx context.Context, table, bucket string, options ...Option) (err error) {
	b := *a
	for _, option := range options {
		option(&b)
	}

	rows, err := b.query(ctx, table)
	if err != nil {
		return err
	}
	defer func() {
		if e := rows.Close(); e != nil && err == nil {
			err = e
		}
	}()

	return b.archive(ctx, rows, table, bucket)
}

// ArchiveRows to S3.
func (a *Archiver) ArchiveRows(rows rows, bucket string, options ...Option) (err error) {
	return a.ArchiveRowsWithContext(context.Background(), rows, bucket)
}

// ArchiveRowsWithContext is like ArchiveRows, with context.
func (a *Archiver) ArchiveRowsWithContext(ctx context.Context, rows rows, bucket string, options ...Option) (err error) {
	b := *a
	for _, option := range options {
		option(&b)
	}

	return b.archive(ctx, rows, "", bucket)
}

type database interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type rows interface {
	ColumnTypes() ([]*sql.ColumnType, error)
	Next() bool
	Scan(...interface{}) error
	Err() error
}

func (a *Archiver) archive(ctx context.Context, rows rows, table, bucket string) (err error) {
	var (
		r, w    = io.Pipe()
		g, gctx = errgroup.WithContext(ctx)
	)
	g.Go(func() error {
		return a.download(gctx, rows, w)
	})
	g.Go(func() error {
		return a.upload(gctx, r, table, bucket)
	})

	return g.Wait()
}

func (a *Archiver) download(ctx context.Context, rows rows, w io.WriteCloser) (err error) {
	defer func() {
		if e := w.Close(); e != nil && err == nil {
			err = e
		}
	}()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	f, err := a.format(w, columns)
	if err != nil {
		return err
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
		select {
		case <-ctx.Done():
			return nil
		default:
			err = rows.Scan(scanned...)
			if err != nil {
				return err
			}

			for i, raw := range rawBytes {
				if raw == nil && a.null != nil {
					record[i] = a.null
				} else {
					record[i] = raw
				}
			}

			if err := f.Format(record); err != nil {
				return err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return nil
}

func (a *Archiver) query(ctx context.Context, table string) (*sql.Rows, error) {
	columns := "*"
	if len(a.columns) > 0 {
		var b strings.Builder
		for i, column := range a.columns {
			b.WriteString(column)
			if i < len(a.columns)-1 {
				b.WriteString(", ")
			}
		}
		columns = b.String()
	}

	query := fmt.Sprintf(`select %s from %s;`, columns, table)
	return a.db.QueryContext(ctx, query)
}

type uploader interface {
	UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

func (a *Archiver) upload(ctx context.Context, r io.ReadCloser, table string, bucket string) (err error) {
	defer func() {
		if e := r.Close(); e != nil && err == nil {
			err = e
		}
	}()

	if a.key == "" {
		if a.extension != "" {
			a.key = fmt.Sprintf("%s.%s", table, a.extension)
		} else {
			a.key = table
		}
	}

	_, err = a.s3.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(a.key),
	})

	return err
}
