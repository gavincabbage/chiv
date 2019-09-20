package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/urfave/cli"

	"gavincabbage.com/chiv"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var version = "v0.0.0"

func main() {

	app := cli.App{
		Name:        "chiv",
		HelpName:    "chiv",
		Usage:       "Archive relational database",
		Version:     version,
		Description: "Ar[chiv]e relational database tables to Amazon S3",
		HideHelp:    true,
		Action:      run,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:     "database, d",
				Usage:    "Database connection string",
				EnvVar:   "DATABASE_URL",
				Required: true,
			},
			cli.StringFlag{
				Name:     "table, t",
				Usage:    "Database table to archive",
				Required: true,
			},
			cli.StringFlag{
				Name:     "bucket, b",
				Usage:    "Upload S3 bucket name",
				Required: true,
			},
			cli.StringFlag{
				Name:     "driver, r",
				Usage:    "Database driver type: postgres or mysql",
				Required: false,
				Value:    "postgres",
			},
			cli.StringSliceFlag{
				Name:  "columns, c",
				Usage: "Database columns to archive, comma-separated",
			},
			cli.StringFlag{
				Name:     "format, f",
				Usage:    "Upload format: csv, yaml or json",
				Required: false,
			},
			cli.StringFlag{
				Name:  "key, k",
				Usage: "Upload key",
			},
			cli.StringFlag{
				Name:  "extension, e",
				Usage: "Upload extension",
			},
			cli.StringFlag{
				Name:  "null, n",
				Usage: "Upload null value",
			},
			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Show usage details",
			},
		},
	}

	cli.HandleExitCoder(app.Run(os.Args))
}

func run(ctx *cli.Context) (err error) {
	defer func() {
		if err != nil {
			err = cli.NewExitError(err, 1)
		}
	}()

	if ctx.Bool("help") {
		return cli.ShowAppHelp(ctx)
	}

	config := from(ctx)

	db, err := sql.Open(config.driver, config.url)
	if err != nil {
		return fmt.Errorf("opening database connection: %w", err)
	}

	awsSession, err := session.NewSessionWithOptions(session.Options{
		Config:            *aws.NewConfig(),
		Profile:           os.Getenv("AWS_PROFILE"),
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return fmt.Errorf("opening AWS session: %w", err)
	}
	client := s3.New(awsSession)
	uploader := s3manager.NewUploaderWithClient(client)

	fmt.Printf("Hello, World! (version %s)\n", version)
	return chiv.Archive(db, uploader, config.table, config.bucket, config.options...)

}

type config struct {
	url     string
	table   string
	bucket  string
	driver  string
	options []chiv.Option
}

func from(ctx *cli.Context) config {
	cfg := config{
		url:    ctx.String("url"),
		table:  ctx.String("table"),
		bucket: ctx.String("bucket"),
		driver: ctx.String("driver"),
	}

	if columns := ctx.StringSlice("columns"); columns != nil {
		cfg.options = append(cfg.options, chiv.WithColumns(columns...))
	}

	var m = map[string]chiv.FormatterFunc{
		"csv":  chiv.CSV,
		"yaml": chiv.YAML,
		"json": chiv.JSON,
	}
	if format := ctx.String("format"); format != "" {
		cfg.options = append(cfg.options, chiv.WithFormat(m[format]))
	}

	if key := ctx.String("key"); key != "" {
		cfg.options = append(cfg.options, chiv.WithKey(key))
	} else if extension := ctx.String("extension"); extension != "" {
		cfg.options = append(cfg.options, chiv.WithExtension(extension))
	}

	if null := ctx.String("null"); null != "" {
		cfg.options = append(cfg.options, chiv.WithNull(null))
	}

	return cfg
}
