![Image](img/chiv.png)

---

<p align="center">
    Archive relational data to Amazon S3.
</p>

<div align="center">
    <a href="https://github.com/gavincabbage/chiv/actions">
        <img src="https://github.com/gavincabbage/chiv/workflows/build/badge.svg" alt="build" />
    </a>
    <a href="https://goreportcard.com/report/gavincabbage.com/chiv">
        <img src="https://goreportcard.com/badge/gavincabbage.com/chiv" alt="go report" />
    </a>
    <a href="https://godoc.org/gavincabbage.com/chiv">
        <img src="https://godoc.org/gavincabbage.com/chiv?status.svg" alt="godoc" />
    </a>
    <a href="https://gavincabbage.com/chiv/blob/master/LICENSE">
        <img src="http://img.shields.io/badge/License-MIT-blue.svg" alt="license" />
    </a>
</div>

---

Provide a database and upload manager to upload a table to an S3 bucket.

```go
db, _ := sql.Open(config.driver, config.url)

client := s3.New(session.NewSessionWithOptions(session.Options{}))
uploader := s3manager.NewUploaderWithClient(client)

chiv.Archive(db, uploader, "table", "bucket")
``` 

Use [options](https://github.com/gavincabbage/chiv/blob/master/options.go) to configure the archival format,
upload key, null placeholder, etc.

```go
chiv.Archive(db, uploader, "table", "bucket"
    chiv.WithFormat(chiv.JSON),
    chiv.WithKey("2019/september/monthly_archive.json"),
    chiv.WithNull("empty"),
)
```

For multiple uploads using the same database and S3 clients, construct an `Archiver`. Options provided during
construction can be overridden in individual archival calls.

```go
a := chiv.NewArchiver(db, uploader, chiv.WithFormat(chiv.YAML))
a.Archive("first_table", "bucket")
a.Archive("second_table", "bucket")
a.Archive("second_table", "bucket", chiv.WithFormat(chiv.JSON), chiv.WithKey("second_table.json"))
``` 

Custom queries can be archived using the `ArchiveRows` family of functions.

```go
rows, _ := db.Exec("SELECT * FROM table and JOIN all the things...")

chiv.ArchiveRows(rows, uploader, "bucket")
``` 

Context-aware versions are also provided, e.g. `ArchiveWithContext`, `ArchiveRowsWithContext`, etc.

## CLI

A simple CLI wrapping the package is also included.

```text
NAME:
   chiv - Archive relational database tables to Amazon S3

USAGE:
   chiv [flags...]

VERSION:
   vX.Y.Z

GLOBAL OPTIONS:
   --database value, -d value   database connection string [$DATABASE_URL]
   --table value, -t value      database table to archive
   --bucket value, -b value     upload S3 bucket name
   --driver value, -r value     database driver type: postgres or mysql (default: "postgres")
   --columns value, -c value    database columns to archive, comma-separated
   --format value, -f value     upload format: csv, yaml or json (default: "csv")
   --key value, -k value        upload key
   --extension value, -e value  upload extension
   --null value, -n value       upload null value
   --help, -h                   show usage details
   --version, -v                print the version

```
