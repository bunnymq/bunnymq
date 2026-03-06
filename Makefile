.PHONY: all build test lint proto clean

GOLANGCI_LINT_VERSION ?= v2.11.3

all: lint build test

build:
	go build ./...

test:
	go test -race ./...

lint:
	golangci-lint run ./...

proto:
	buf generate

clean:
	go clean ./...
