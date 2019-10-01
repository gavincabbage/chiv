// +build unit

package chiv_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"gavincabbage.com/chiv"
)

type testCase struct {
	name    string
	columns []column
	records [][][]byte
}

var i interface{}

var cases = []testCase{
	{
		name: "base case",
		columns: []column{
			{
				name:         "first_column",
				databaseType: "INTEGER",
				scanType:     reflect.TypeOf(0),
			},
			{
				name:         "second_column",
				databaseType: "TEXT",
				scanType:     reflect.TypeOf(""),
			},
			{
				name:         "third_column",
				databaseType: "FLOAT",
				scanType:     reflect.TypeOf(0.0),
			},
			{
				name:         "fourth_column",
				databaseType: "INTEGER",
				scanType:     reflect.TypeOf(i),
			},
		},
		records: [][][]byte{
			{
				[]byte("1"),
				[]byte("first_row"),
				[]byte("100"),
				[]byte("6"),
			},
			{
				[]byte("2"),
				[]byte("second_row"),
				[]byte("12.12"),
				[]byte("7"),
			},
			{
				[]byte("3"),
				[]byte("third_row"),
				[]byte("42.42"),
				[]byte("8"),
			},
		},
	},
}

func TestCsvFormatter(t *testing.T) {
	expected := []string{`
first_column,second_column,third_column,fourth_column
1,first_row,100,6
2,second_row,12.12,7
3,third_row,42.42,8
`,
	}

	test(t, expected, chiv.CSV)
}

func TestYamlFormatter(t *testing.T) {
	expected := []string{`
- first_column: 1
  fourth_column: 6
  second_column: first_row
  third_column: 100
- first_column: 2
  fourth_column: 7
  second_column: second_row
  third_column: 12.12
- first_column: 3
  fourth_column: 8
  second_column: third_row
  third_column: 42.42
`,
	}

	test(t, expected, chiv.YAML)
}

func TestJsonFormatter(t *testing.T) {
	expected := []string{`
[{"first_column":1,"fourth_column":6,"second_column":"first_row","third_column":100},{"first_column":2,"fourth_column":7,"second_column":"second_row","third_column":12.12},{"first_column":3,"fourth_column":8,"second_column":"third_row","third_column":42.42}]`,
	}

	test(t, expected, chiv.JSON)
}

func test(t *testing.T, expected []string, format chiv.FormatterFunc) {
	for i, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var (
				b       = bytes.Buffer{}
				columns = make([]chiv.Column, len(test.columns))
			)
			for i := range test.columns {
				columns[i] = test.columns[i]
			}

			subject, err := format(&b, columns)
			assert.NoError(t, err)

			for _, record := range test.records {
				assert.NoError(t, subject.Format(record))
			}

			assert.NoError(t, subject.Close())
			assert.Equal(t, expected[i][1:], b.String())
		})
	}
}

type column struct {
	databaseType string
	name         string
	scanType     reflect.Type
}

func (c column) DatabaseTypeName() string {
	return c.databaseType
}

func (c column) Name() string {
	return c.name
}

func (c column) ScanType() reflect.Type {
	return c.scanType
}
