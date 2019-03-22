// Package chiv_test includes integration tests external to package chiv
// and relies on external services postgres and s3 (localstack) via CodeShip.
package chiv_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gavincabbage/chiv"
)

func TestArchive(t *testing.T) {
	var (
		db         = newDB(t)
		s3client   = newS3(t)
		uploader   = s3manager.NewUploaderWithClient(s3client)
		downloader = s3manager.NewDownloaderWithClient(s3client)
	)

	mustExec(t, db, `
	CREATE TABLE IF NOT EXISTS "test_table" (
		id UUID PRIMARY KEY,
		text_column TEXT,
		char_column VARCHAR(50),
		int_column INTEGER,
		bool_column BOOLEAN,
		ts_column TIMESTAMP
	);`)
	defer mustExec(t, db, `DROP TABLE "test_table";`)

	mustExec(t, db, `
	INSERT INTO "test_table" VALUES (
		'ea09d13c-f441-4550-9492-115f8b409c96',
		'some text',
		'some chars',
		42,
		true,
		'2018-01-04'::timestamp
	);`)

	mustExec(t, db, `
	INSERT INTO "test_table" VALUES (
		'7530a381-526a-42aa-a9ba-97fb2bca283f',
		'some more text',
		'some more chars',
		101,
		false,
		'2018-02-05'::timestamp
	);`)

	expected := `id,text_column,char_column,int_column,bool_column,ts_column
ea09d13c-f441-4550-9492-115f8b409c96,some text,some chars,42,true,SOMETIMESTAMP
7530a381-526a-42aa-a9ba-97fb2bca283f,some more text,some more chars,101,false,OTHERTIMESTAMP`

	if _, err := s3client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("test_bucket"),
	}); err != nil {
		t.Error(err)
	}

	subject := chiv.NewArchiver(db, uploader)
	assert.NotNil(t, subject)

	err := subject.Archive("test_table", "test_bucket")
	require.NoError(t, err)

	b := &aws.WriteAtBuffer{}
	n, err := downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String("test_bucket"),
		Key:    aws.String("test_table.csv"),
	})
	require.NoError(t, err)
	require.Equal(t, len([]byte(expected)), n)
	require.Equal(t, expected, string(b.Bytes()))
}

func newDB(t *testing.T) *sql.DB {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	require.NoError(t, err)

	return db
}

func newS3(t *testing.T) *s3.S3 {
	awsConfig := aws.NewConfig().
		WithRegion(os.Getenv("AWS_REGION")).
		WithDisableSSL(true).
		WithCredentials(credentials.NewEnvCredentials())

	awsSession, err := session.NewSession(awsConfig)
	require.NoError(t, err)

	client := s3.New(awsSession)
	client.Endpoint = os.Getenv("AWS_ENDPOINT")

	return client
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Error(err)
	}
}
