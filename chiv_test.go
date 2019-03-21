// Package chiv_test includes integration tests external to package chiv
// and relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"testing"

	"github.com/gavincabbage/chiv"
)

func TestArchive(t *testing.T) {
	chiv.Archive()
	t.Log("test test test")
}
