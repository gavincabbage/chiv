GOTOOLS = \
	github.com/golangci/golangci-lint/cmd/golangci-lint \
	golang.org/x/tools/cmd/cover \

.PHONY: setup
setup:
	go get $(GOTOOLS)
	go mod download

.PHONY: install
install:
	@echo not yet implemented

.PHONY: test
test:
	echo 'mode: atomic' > cover.out && go test -p 1 -tags test -coverprofile cover.out -covermode=atomic -timeout=30s ./...

.PHONY: cover
cover: test
	go tool cover -html=cover.out

.PHONY: lint
lint:
	golangci-lint run

.PHONY: clean
clean:
	rm cover.out
