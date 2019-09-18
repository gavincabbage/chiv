// +build unit

package chiv_test

import (
	"database/sql"
	"testing"
)

func TestArchiver(t *testing.T) {

}

type database struct {
	rows *sql.Rows
}

type uploader struct {
}
