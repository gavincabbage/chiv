package chiv

type Option func(*Archiver)

type Format int

const (
	CSV Format = iota
	JSON
)

func WithFormat(f Format) Option {
	return func(a *Archiver) {
		a.format = f
	}
}
