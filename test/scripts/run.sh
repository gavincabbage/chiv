#!/usr/bin/env sh
# docker-compose -f docker-compose.yml up --exit-code-from test --build

set -euxo pipefail

# lint
golangci-lint run

# test
go test -p 1 -covermode=atomic -timeout=30s ./...

# benchmark
go test -run=Benchmark -bench=. -benchmem
