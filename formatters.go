package chiv

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"regexp"
	"strconv"

	yaml "gopkg.in/yaml.v2"
)

// FormatterFunc returns an initialized Formatter.
type FormatterFunc func(io.Writer, []*sql.ColumnType) (Formatter, error)

// Formatter formats and writes records.
type Formatter interface {
	Format([][]byte) error
	Close() error
}

type csvFormatter struct {
	w     *csv.Writer
	count int
}

// CSV returns an initialized csvFormatter.
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

// Format a CSV record.
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

// Close and flush the CSV formatter.
func (f *csvFormatter) Close() error {
	f.w.Flush()
	return f.w.Error()
}

type yamlFormatter struct {
	w       io.Writer
	columns []*sql.ColumnType
	parser  *parser
}

// YAML returns an initialized yamlFormatter.
func YAML(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	p, err := newParser()
	if err != nil {
		return nil, err
	}

	f := yamlFormatter{
		w:       w,
		columns: columns,
		parser:  p,
	}

	return &f, nil
}

// Format a YAML record.
func (f *yamlFormatter) Format(record [][]byte) error {
	if len(f.columns) != len(record) {
		return ErrRecordLength
	}

	m, err := buildMap(record, f.columns, f.parser)
	if err != nil {
		return err
	}
	l := []map[string]interface{}{m}

	if err := write(l, f.w, yaml.Marshal); err != nil {
		return err
	}

	return nil
}

// Close the yamlFormatter.
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
	parser   *parser
}

// JSON returns an initialized jsonFormatter with an open JSON array.
func JSON(w io.Writer, columns []*sql.ColumnType) (Formatter, error) {
	p, err := newParser()
	if err != nil {
		return nil, err
	}

	f := jsonFormatter{
		w:       w,
		columns: columns,
		parser:  p,
	}

	if err := f.writeByte(openBracket); err != nil {
		return nil, err
	}

	return &f, nil
}

// Format a JSON record.
func (f *jsonFormatter) Format(record [][]byte) error {
	if len(f.columns) != len(record) {
		return ErrRecordLength
	}

	m, err := buildMap(record, f.columns, f.parser)
	if err != nil {
		return err
	}

	if f.notFirst {
		err := f.writeByte(comma)
		if err != nil {
			return err
		}
	}

	if err := write(m, f.w, json.Marshal); err != nil {
		return err
	}

	f.notFirst = true
	return nil
}

// Close the jsonFormatter after closing the JSON array.
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

type parser struct {
	boolRegex    *regexp.Regexp
	intRegex     *regexp.Regexp
	decimalRegex *regexp.Regexp
}

func newParser() (*parser, error) {
	var (
		boolRegex, boolErr       = regexp.Compile("BOOL*")
		intRegex, intErr         = regexp.Compile("INT*")
		decimalRegex, decimalErr = regexp.Compile("DECIMAL*|FLOAT*|NUMERIC*")
	)

	if boolErr != nil || intErr != nil || decimalErr != nil {
		return nil, ErrParserRegex
	}

	p := parser{
		boolRegex:    boolRegex,
		intRegex:     intRegex,
		decimalRegex: decimalRegex,
	}

	return &p, nil
}

func (p *parser) parse(b []byte, t string) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var (
		s = string(b)
	)
	switch {
	case p.boolRegex.MatchString(t):
		return strconv.ParseBool(s)
	case p.intRegex.MatchString(t):
		return strconv.Atoi(s)
	case p.decimalRegex.MatchString(t):
		return strconv.ParseFloat(s, 64)
	default:
		return s, nil
	}
}

func buildMap(record [][]byte, columns []*sql.ColumnType, p *parser) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	for i, column := range columns {
		r, err := p.parse(record[i], columns[i].DatabaseTypeName())
		if err != nil {
			return nil, err
		}
		m[column.Name()] = r
	}

	return m, nil
}

type marshalFunc func(interface{}) ([]byte, error)

func write(v interface{}, w io.Writer, marshal marshalFunc) error {
	b, err := marshal(v)
	if err != nil {
		return err
	}

	n, err := w.Write(b)
	if err != nil {
		return err
	} else if n != len(b) {
		return io.ErrShortWrite
	}

	return nil
}
