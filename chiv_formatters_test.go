// +build unit

package chiv_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"gavincabbage.com/chiv"
)

type testCase struct {
	name    string
	columns []column
	records [][][]byte
}

var cases = []testCase{
	{
		name: "base case",
		columns: []column{
			{
				name:         "first_column",
				databaseType: "INTEGER",
			},
			{
				name:         "second_column",
				databaseType: "TEXT",
			},
		},
		records: [][][]byte{
			{
				[]byte("1"),
				[]byte("first_row"),
			},
			{
				[]byte("2"),
				[]byte("second_row"),
			},
			{
				[]byte("3"),
				[]byte("third_row"),
			},
		},
	},
}

func TestCsvFormatter(t *testing.T) {
	expected := []string{
		`first_column,second_column
1,first_row
2,second_row
3,third_row
`,
	}

	test(t, expected, chiv.CSV)
}

func TestYamlFormatter(t *testing.T) {
	expected := []string{
		`- first_column: 1
  second_column: first_row
- first_column: 2
  second_column: second_row
- first_column: 3
  second_column: third_row
`,
	}

	test(t, expected, chiv.YAML)
}

func TestJsonFormatter(t *testing.T) {
	expected := []string{
		`[{"first_column":1,"second_column":"first_row"},{"first_column":2,"second_column":"second_row"},{"first_column":3,"second_column":"third_row"}]`,
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
			assert.Equal(t, expected[i], b.String())
		})
	}
}

type column struct {
	databaseType string
	name         string
}

func (c column) DatabaseTypeName() string {
	return c.databaseType
}

func (c column) Name() string {
	return c.name
}
