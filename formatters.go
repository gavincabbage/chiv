package chiv

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
)

const (
	openBracket  = byte('[')
	closeBracket = byte(']')
	comma        = byte(',')
)

var (
	// ErrRecordLength does not match the number of columns.
	ErrRecordLength = errors.New("record length does not match number of columns")
)

type formatter interface {
	Begin([]*sql.ColumnType) error
	Write([]sql.RawBytes) error
	End() error
}

// csvFormatter formats columns in CSV format.
type csvFormatter struct {
	w     *csv.Writer
	count int
}

func (c *csvFormatter) Begin(columns []*sql.ColumnType) error {
	c.count = len(columns)

	header := make([]string, 0, c.count)
	for _, column := range columns {
		header = append(header, column.Name())
	}

	return c.w.Write(header)
}

func (c *csvFormatter) Write(record []sql.RawBytes) error {
	if c.count != len(record) {
		return ErrRecordLength
	}

	strings := make([]string, c.count)
	for i, item := range record {
		strings[i] = string(item)
	}

	return c.w.Write(strings)
}

func (c *csvFormatter) End() error {
	c.w.Flush()
	return c.w.Error()
}

// yamlFormatter formats columns in YAML format.
type yamlFormatter struct {
	columns []*sql.ColumnType
}

func (c *yamlFormatter) Begin(columns []*sql.ColumnType) error {
	return nil
}

func (c *yamlFormatter) Write(record []sql.RawBytes) error {
	if len(c.columns) != len(record) {
		return ErrRecordLength
	}

	return nil
}

func (c *yamlFormatter) End() error {
	return nil
}

// jsonFormatter formats columns in JSON format.
type jsonFormatter struct {
	w        io.Writer
	columns  []*sql.ColumnType
	notFirst bool
}

func (c *jsonFormatter) Begin(columns []*sql.ColumnType) error {
	c.columns = columns
	return writeByte(c.w, openBracket)
}

func (c *jsonFormatter) Write(record []sql.RawBytes) error {
	if len(c.columns) != len(record) {
		return ErrRecordLength
	}

	m := make(map[string]interface{})
	for i, column := range c.columns {
		r, err := parse(record[i], c.columns[i].DatabaseTypeName())
		if err != nil {
			return err
		}
		m[column.Name()] = r
	}

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if c.notFirst {
		err := writeByte(c.w, comma)
		if err != nil {
			return err
		}
	}

	n, err := c.w.Write(b)
	if err != nil {
		return err
	} else if n != len(b) {
		return io.ErrShortWrite
	}

	c.notFirst = true
	return nil
}

func (c *jsonFormatter) End() error {
	return writeByte(c.w, closeBracket)
}

func writeByte(w io.Writer, b byte) error {
	n, err := w.Write([]byte{b})
	if err != nil {
		return err
	} else if n != 1 {
		return io.ErrShortWrite
	}

	return nil
}

func parse(b sql.RawBytes, t string) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var (
		s            = string(b)
		boolRegex    = regexp.MustCompile("BOOL*")
		intRegex     = regexp.MustCompile("INT*")
		decimalRegex = regexp.MustCompile("DECIMAL*")
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
