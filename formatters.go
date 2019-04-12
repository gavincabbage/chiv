package chiv

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"regexp"
	"strconv"
)

// FormatterFunc returns an initialized Formatter.
type FormatterFunc func(io.Writer, []*sql.ColumnType) (Formatter, error)

// Formatter formats and writes records.
type Formatter interface {
	Format([][]byte) error
	Close() error
}

func CSV(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	f := &csvFormatter{
		w:     csv.NewWriter(w),
		count: len(columns),
	}

	header := make([]string, f.count)
	for i, column := range columns {
		header[i] = column.Name()
	}

	if err := f.w.Write(header); err != nil {
		return nil, err
	}

	return f, nil
}

// csvFormatter formats columns in CSV format.
type csvFormatter struct {
	w     *csv.Writer
	count int
}

func (f *csvFormatter) Format(record [][]byte) error {
	if f.count != len(record) {
		return ErrRecordLength
	}

	strings := make([]string, f.count)
	for i, item := range record {
		strings[i] = string(item)
	}

	return f.w.Write(strings)
}

func (f *csvFormatter) Close() error {
	f.w.Flush()
	return f.w.Error()
}

func YAML(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	return &yamlFormatter{columns: columns}, nil
}

// yamlFormatter formats columns in YAML format.
type yamlFormatter struct {
	columns []*sql.ColumnType
}

func (c *yamlFormatter) Format(record [][]byte) error {
	if len(c.columns) != len(record) {
		return ErrRecordLength
	}

	return nil
}

func (c *yamlFormatter) Close() error {
	return nil
}

const (
	openBracket  = byte('[')
	closeBracket = byte(']')
	comma        = byte(',')
)

func JSON(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	f := &jsonFormatter{
		w:       w,
		columns: columns,
	}

	if err := f.writeByte(openBracket); err != nil {
		return nil, err
	}

	return f, nil
}

// jsonFormatter formats columns in JSON format.
type jsonFormatter struct {
	w        io.Writer
	columns  []*sql.ColumnType
	notFirst bool
}

func (f *jsonFormatter) Format(record [][]byte) error {
	if len(f.columns) != len(record) {
		return ErrRecordLength
	}

	m := make(map[string]interface{})
	for i, column := range f.columns {
		r, err := parse(record[i], f.columns[i].DatabaseTypeName())
		if err != nil {
			return err
		}
		m[column.Name()] = r
	}

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if f.notFirst {
		err := f.writeByte(comma)
		if err != nil {
			return err
		}
	}

	n, err := f.w.Write(b)
	if err != nil {
		return err
	} else if n != len(b) {
		return io.ErrShortWrite
	}

	f.notFirst = true
	return nil
}

func (f *jsonFormatter) Close() error {
	return f.writeByte(closeBracket)
}

func (f *jsonFormatter) writeByte(b byte) error {
	n, err := f.w.Write([]byte{b})
	if err != nil {
		return err
	} else if n != 1 {
		return io.ErrShortWrite
	}

	return nil
}

func parse(b []byte, t string) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var (
		s            = string(b)
		boolRegex    = regexp.MustCompile("BOOL*")
		intRegex     = regexp.MustCompile("INT*")
		decimalRegex = regexp.MustCompile("DECIMAL*|FLOAT*|NUMERIC*")
	)
	switch {
	case boolRegex.MatchString(t):
		return strconv.ParseBool(s)
	case intRegex.MatchString(t):
		return strconv.Atoi(s)
	case decimalRegex.MatchString(t):
		return strconv.ParseFloat(s, 64)
	default:
		return s, nil
	}
}
