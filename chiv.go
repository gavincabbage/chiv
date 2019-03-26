// Package chiv archives arbitrarily large relational database tables to Amazon S3.
package chiv

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	key    string
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
func (a *Archiver) Archive(table, bucket string, options ...Option) error {
	return a.ArchiveWithContext(context.Background(), table, bucket, options...)
}

// Archive a database table to S3 with context.
func (a *Archiver) ArchiveWithContext(ctx context.Context, table, bucket string, options ...Option) error {
	archiver := archiver{
		db:     a.db,
		s3:     a.s3,
		ctx:    ctx,
		config: a,
	}

	for _, option := range options {
		option(archiver.config)
	}

	return archiver.archive(table, bucket)
}

type archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	ctx    context.Context
	config *Archiver
}

func (a *archiver) archive(table, bucket string) error {
	errs := make(chan error)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	go func() {
		cw := csv.NewWriter(w)

		selectAll := fmt.Sprintf(`select * from "%s";`, table)
		rows, err := a.db.QueryContext(a.ctx, selectAll)
		if err != nil {
			errs <- err
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			errs <- err
			return
		}

		if err := cw.Write(columns); err != nil {
			errs <- err
			return
		}

		var (
			rawBytes = make([]sql.RawBytes, len(columns))
			record   = make([]string, len(columns))
			dest     = make([]interface{}, len(columns))
		)
		for i := range rawBytes {
			dest[i] = &rawBytes[i]
		}

		for rows.Next() {
			err = rows.Scan(dest...)
			if err != nil {
				errs <- err
				return
			}

			for i, raw := range rawBytes {
				if raw == nil {
					record[i] = "\\N"
				} else {
					record[i] = string(raw)
				}
			}

			if err := cw.Write(record); err != nil {
				errs <- err
				return
			}
		}

		if err := rows.Err(); err != nil {
			errs <- err
			return
		}

		cw.Flush()
		if err := cw.Error(); err != nil {
			errs <- err
			return
		}

		if err := w.Close(); err != nil {
			errs <- err
			return
		}
	}()

	go func() {
		if a.config.key == "" {
			switch a.config.format {
			case CSV:
				a.config.key = fmt.Sprintf("%s.csv", table)
			case JSON:
				a.config.key = fmt.Sprintf("%s.json", table)
			}
		}

		if _, err := a.s3.UploadWithContext(a.ctx, &s3manager.UploadInput{
			Body:   r,
			Bucket: aws.String(bucket),
			Key:    aws.String(a.config.key),
		}); err != nil {
			errs <- err
		}

		errs <- nil
	}()

	select {
	case err := <-errs:
		return err
	case <-a.ctx.Done():
		return nil
	}
}
