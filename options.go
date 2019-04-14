package chiv

// Option configures the Archiver. Options can be provided when creating an Archiver or on each call to Archive.
type Option func(*archiver)

// WithFormat configures the upload format.
func WithFormat(f FormatterFunc) Option {
	return func(a *archiver) {
		a.format = f
	}
}

// WithKey configures the object key uploaded to S3.
func WithKey(s string) Option {
	return func(a *archiver) {
		a.key = s
	}
}

// WithExtension configures an extension for object keys uploaded to S3.
func WithExtension(s string) Option {
	return func(a *archiver) {
		a.extension = s
	}
}

// WithNull configures a custom null string.
func WithNull(s string) Option {
	return func(a *archiver) {
		a.null = []byte(s)
	}
}
