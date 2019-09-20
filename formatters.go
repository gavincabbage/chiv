package chiv

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"

	yaml "gopkg.in/yaml.v2"
)

// FormatterFunc returns an initialized Formatter.
type FormatterFunc func(io.Writer, []*sql.ColumnType) (Formatter, error)

// Formatter formats and writes records.
type Formatter interface {
	// Format and write a single record.
	Format([][]byte) error
	// Close the formatter and perform any format-specific cleanup operations.
	Close() error
}

type csvFormatter struct {
	w     *csv.Writer
	count int
}

// CSV writes column headers and returns an initialized CSV formatter.
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
		return nil, fmt.Errorf("writing header: %w", err)
	}

	return f, nil
}

// Format a CSV record.
func (f *csvFormatter) Format(record [][]byte) error {
	if f.count != len(record) {
		return errors.New("record length does not match number of columns")
	}

	strings := make([]string, f.count)
	for i, item := range record {
		strings[i] = string(item)
	}

	return f.w.Write(strings)
}

// Close and flush the CSV formatter.
func (f *csvFormatter) Close() error {
	f.w.Flush()
	if err := f.w.Error(); err != nil {
		return fmt.Errorf("closing csv formatter: %w", err)
	}

	return nil
}

type yamlFormatter struct {
	w       io.Writer
	columns []*sql.ColumnType
}

// YAML returns an initialized YAML formatter.
func YAML(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	f := yamlFormatter{
		w:       w,
		columns: columns,
	}

	return &f, nil
}

// Format a YAML record.
func (f *yamlFormatter) Format(record [][]byte) error {
	if len(f.columns) != len(record) {
		return errors.New("record length does not match number of columns")
	}

	m, err := buildMap(record, f.columns)
	if err != nil {
		return fmt.Errorf("transforming data: %w", err)
	}
	l := []map[string]interface{}{m}

	if err := write(l, f.w, yaml.Marshal); err != nil {
		return fmt.Errorf("writing formatted data: %w", err)
	}

	return nil
}

// Close the YAML formatter.
func (f *yamlFormatter) Close() error {
	return nil
}

const (
	openBracket  = byte('[')
	closeBracket = byte(']')
	comma        = byte(',')
)

type jsonFormatter struct {
	w        io.Writer
	columns  []*sql.ColumnType
	notFirst bool
}

// JSON opens a JSON array and returns an initialized JSON formatter.
func JSON(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	f := jsonFormatter{
		w:       w,
		columns: columns,
	}

	if err := f.writeByte(openBracket); err != nil {
		return nil, fmt.Errorf("writing json: %w", err)
	}

	return &f, nil
}

// Format a JSON record.
func (f *jsonFormatter) Format(record [][]byte) error {
	if len(f.columns) != len(record) {
		return errors.New("record length does not match number of columns")
	}

	m, err := buildMap(record, f.columns)
	if err != nil {
		return fmt.Errorf("transforming data: %w", err)
	}

	if f.notFirst {
		err := f.writeByte(comma)
		if err != nil {
			return fmt.Errorf("writing json: %w", err)
		}
	}

	if err := write(m, f.w, json.Marshal); err != nil {
		return fmt.Errorf("writing formatted data: %w", err)
	}

	f.notFirst = true
	return nil
}

// Close the jsonFormatter after closing the JSON array.
func (f *jsonFormatter) Close() error {
	if err := f.writeByte(closeBracket); err != nil {
		return fmt.Errorf("closing json formatter: %w", err)
	}

	return nil
}

func (f *jsonFormatter) writeByte(b byte) error {
	_, err := f.w.Write([]byte{b})
	if err != nil {
		return err
	}

	return nil
}

var pattern = struct {
	boolean, integer, float *regexp.Regexp
}{
	boolean: regexp.MustCompile("BOOL*"),
	integer: regexp.MustCompile("INT*"),
	float:   regexp.MustCompile("DECIMAL*|FLOAT*|NUMERIC*|DOUBLE*"),
}

func parse(v []byte, t string) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	s := string(v)
	switch {
	case pattern.boolean.MatchString(t):
		return strconv.ParseBool(s)
	case pattern.integer.MatchString(t):
		return strconv.Atoi(s)
	case pattern.float.MatchString(t):
		return strconv.ParseFloat(s, 64)
	default:
		return s, nil
	}
}

func buildMap(record [][]byte, columns []*sql.ColumnType) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for i, column := range columns {
		r, err := parse(record[i], column.DatabaseTypeName())
		if err != nil {
			return nil, err
		}
		m[column.Name()] = r
	}

	return m, nil
}

type marshaller func(interface{}) ([]byte, error)

func write(v interface{}, w io.Writer, m marshaller) error {
	b, err := m(v)
	if err != nil {
		return err
	}

	if _, err := w.Write(b); err != nil {
		return err
	}

	return nil
}
