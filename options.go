package chiv

// Option configures the Archiver. Options can be provided when creating an Archiver or on each call to Archive.
type Option func(*config)

// WithFormat configures the upload format.
func WithFormat(f FormatterFunc) Option {
	return func(c *config) {
		c.format = f
	}
}

// WithKey configures the upload object key in S3.
func WithKey(s string) Option {
	return func(c *config) {
		c.key = s
	}
}

// WithNull configures a custom null string.
func WithNull(s string) Option {
	return func(c *config) {
		c.null = []byte(s)
	}
}
