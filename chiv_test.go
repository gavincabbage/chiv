// +build unit

package chiv_test

import (
	"database/sql"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/stretchr/testify/require"

	"gavincabbage.com/chiv"
)

func TestArchiveRows(t *testing.T) {
	cases := []struct {
		name        string
		rows        *rows
		uploader    *uploader
		bucket      string
		formatter   *formatter
		options     []chiv.Option
		expectedErr string
	}{
		{
			name:      "base case",
			rows:      &rows{},
			uploader:  &uploader{},
			formatter: &formatter{},
		},
		{
			name: "happy path one row",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
			},
			uploader:  &uploader{},
			formatter: &formatter{},
		},
		{
			name: "happy path multiple rows",
			rows: &rows{
				columns: []string{"first_column", "second_column", "third_column"},
				scan: [][]string{
					{"first", "second", "third"},
					{"fourth", "fifth", "sixth"},
					{"seventh", "eighth", "ninth"},
				},
			},
			uploader:  &uploader{},
			formatter: &formatter{},
		},
		{
			name: "column types error",
			rows: &rows{
				columns:        []string{"first_column", "second_column"},
				scan:           [][]string{{"first", "second"}},
				columnTypesErr: errors.New("column types"),
			},
			expectedErr: "chiv: getting column types from rows: column types",
			uploader:    &uploader{},
			formatter:   &formatter{},
		},
		{
			name: "formatter func error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
			},
			expectedErr: "chiv: downloading: opening formatter: opening formatter",
			uploader:    &uploader{},
			formatter: &formatter{
				openErr: errors.New("opening formatter"),
			},
		},
		{
			name: "scan error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
				scanErr: errors.New("scanning"),
			},
			expectedErr: "chiv: downloading: scanning row: scanning",
			uploader:    &uploader{},
			formatter:   &formatter{},
		},
		{
			name: "formatter error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
			},
			expectedErr: "chiv: downloading: formatting row: formatting",
			uploader:    &uploader{},
			formatter: &formatter{
				formatErr: errors.New("formatting"),
			},
		},
		{
			name: "db error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
				errErr:  errors.New("database"),
			},
			expectedErr: "chiv: downloading: scanning rows: database",
			uploader:    &uploader{},
			formatter:   &formatter{},
		},
		{
			name: "formatter close error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
			},
			expectedErr: "chiv: downloading: closing formatter: closing formatter",
			uploader:    &uploader{},
			formatter: &formatter{
				closeErr: errors.New("closing formatter"),
			},
		},
		{
			name: "upload error",
			rows: &rows{
				columns: []string{"first_column", "second_column"},
				scan:    [][]string{{"first", "second"}},
			},
			expectedErr: "chiv: uploading: uploading",
			uploader: &uploader{
				uploadErr: errors.New("uploading"),
			},
			formatter: &formatter{},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var (
				options = append(test.options, chiv.WithFormat(format(test.formatter)))
				err     = chiv.ArchiveRows(test.rows, test.uploader, "bucket", options...)
			)

			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
				return
			}

			require.NoError(t, err)
			require.True(t, test.formatter.closed)

			for i := range test.rows.scan {
				for j := range test.rows.scan[i] {
					require.True(t, i < len(test.formatter.written) && j < len(test.formatter.written[i]), "formatter written record count")
					expected := test.rows.scan[i][j]
					actual := test.formatter.written[i][j]
					require.Equal(t, expected, actual)
				}
			}
		})
	}
}

type rows struct {
	columns []string
	scan    [][]string
	scanNdx int

	columnTypesErr, scanErr, errErr error
}

func (r *rows) ColumnTypes() ([]*sql.ColumnType, error) {
	return make([](*sql.ColumnType), len(r.columns)), r.columnTypesErr
}

func (r *rows) Next() bool {
	return r.scanNdx < len(r.scan)
}

func (r *rows) Scan(c ...interface{}) error {
	if r.scanErr != nil {
		return r.scanErr
	}

	s := r.scan[r.scanNdx]
	for i := range s {
		if v, ok := c[i].(*sql.RawBytes); ok {
			*v = sql.RawBytes(s[i])
		}
	}

	r.scanNdx++
	return nil
}

func (r *rows) Err() error {
	return r.errErr
}

type uploader struct {
	uploadErr error
}

func (u *uploader) UploadWithContext(ctx aws.Context, input *s3manager.UploadInput, opts ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	p := make([]byte, 1)
	for {
		if _, err := input.Body.Read(p); err != nil {
			break
		}
	}

	return nil, u.uploadErr
}

func format(f chiv.Formatter) chiv.FormatterFunc {
	return func(_ io.Writer, _ []chiv.Column) chiv.Formatter {
		return f
	}
}

type formatter struct {
	closed                       bool
	written                      [][]string
	openErr, formatErr, closeErr error
}

func (f *formatter) Open() error {
	return f.openErr
}

func (f *formatter) Format(record [][]byte) error {
	if f.formatErr != nil {
		return f.formatErr
	}

	var s []string
	for _, b := range record {
		s = append(s, string(b))
	}

	f.written = append(f.written, s)

	return nil
}
func (f *formatter) Close() error {
	f.closed = true
	return f.closeErr
}
