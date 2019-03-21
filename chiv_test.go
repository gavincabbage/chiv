// Package chiv_test includes integration tests external to package chiv
// and relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"database/sql"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gavincabbage/chiv"
)

func TestArchive(t *testing.T) {
	db := newDB(t)
	uploader := newUploader(t)

	mustExec(t, db, `
	CREATE TABLE IF NOT EXISTS "test_table" (
		id UUID PRIMARY KEY,
		text_column TEXT,
		char_column VARCHAR(10),
		int_column INTEGER,
		bool_column BOOLEAN,
		ts_column TIMESTAMP
	);`)
	defer mustExec(t, db, `DROP TABLE "test_table";`)

	subject := chiv.NewArchiver(db, uploader)
	assert.NotNil(t, subject)
}

func newDB(t *testing.T) *sql.DB {
	// TODO make this generic so it can be run locally as well
	const databaseURL = "postgres://postgres@postgres/test?sslmode=disable"

	db, err := sql.Open("postgres", databaseURL)
	require.NoError(t, err)

	return db
}

func newUploader(t *testing.T) *s3manager.Uploader {
	const (
		awsRegion  = "us-east-1"
		s3Endpoint = "http://s3:4572"
	)

	awsConfig := aws.NewConfig().
		WithRegion(awsRegion).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	require.NoError(t, err)

	client := s3.New(awsSession)
	client.Endpoint = s3Endpoint

	return s3manager.NewUploaderWithClient(client)
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Error(err)
	}
}
