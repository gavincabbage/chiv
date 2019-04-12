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
)

// Archiver archives arbitrarily large relational database tables to Amazon S3. It contains a database connection
// and upload client. Options set on creation apply to all calls to Archive unless overridden.
type Archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	config config
}

type config struct {
	format FormatterFunc
	key    string
	null   []byte
}

// NewArchiver constructs an Archiver with the given database connection, S3 uploader and options.
func NewArchiver(db *sql.DB, s3 *s3manager.Uploader, options ...Option) *Archiver {
	a := Archiver{
		db: db,
		s3: s3,
		config: config{
			format: DefaultFormatFunc,
		},
	}

	for _, option := range options {
		option(&a.config)
	}

	return &a
}

// Archive a database table to S3.
func (a *Archiver) Archive(table, bucket string, options ...Option) error {
	return a.ArchiveWithContext(context.Background(), table, bucket, options...)
}

// ArchiveWithContext is like Archive, with context.
func (a *Archiver) ArchiveWithContext(ctx context.Context, table, bucket string, options ...Option) error {
	archiver := archiver{
		db:     a.db,
		s3:     a.s3,
		ctx:    ctx,
		config: a.config,
	}

	for _, option := range options {
		option(&archiver.config)
	}

	return archiver.archive(table, bucket)
}

type archiver struct {
	db     *sql.DB
	s3     *s3manager.Uploader
	ctx    context.Context
	config config
}

func (a *archiver) archive(table string, bucket string) error {
	errs := make(chan error)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	go a.download(w, table, errs)
	go a.upload(r, table, bucket, errs)

	select {
	case err := <-errs:
		return err
	case <-a.ctx.Done():
		return nil
	}
}

func (a *archiver) download(wc io.WriteCloser, table string, errs chan error) {
	selectAll := fmt.Sprintf(`select * from "%s";`, table)
	rows, err := a.db.QueryContext(a.ctx, selectAll)
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

	f, err := a.config.format(wc, columns)
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
			if raw == nil && a.config.null != nil {
				record[i] = a.config.null
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

func (a *archiver) upload(r io.Reader, table string, bucket string, errs chan error) {
	if a.config.key == "" {
		// TODO if a.config.extension or something? can pass in '.json'? wish i could connect to formatter hm
		a.config.key = table
	}

	if _, err := a.s3.UploadWithContext(a.ctx, &s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String(bucket),
		Key:    aws.String(a.config.key),
	}); err != nil {
		errs <- err
	}

	errs <- nil
}
