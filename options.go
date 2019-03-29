package chiv

// Option configures the Archiver. Options can be provided when creating an Archiver or on each call to Archive.
type Option func(*Archiver)

// Format uploaded to S3.
type Format int

const (
	// CSV file format.
	CSV Format = iota
	// YAML file format.
	YAML
	// JSON file format.
	JSON
)

// WithFormat configures the upload format.
func WithFormat(f Format) Option {
	return func(a *Archiver) {
		a.format = f
	}
}

// WithKey configures the upload object key in S3.
func WithKey(s string) Option {
	return func(a *Archiver) {
		a.key = s
	}
}

// WithNull configures a custom null string.
func WithNull(s string) Option {
	return func(a *Archiver) {
		a.null = []byte(s)
	}
}
