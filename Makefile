.PHONY: all build test integration-test integration-test-local lint proto clean docker-build cluster-up cluster-down cluster-logs

GOLANGCI_LINT_VERSION ?= v2.11.3

all: lint build test

build:
	go build ./...

test:
	go test -race ./...

integration-test:
	bash scripts/run-integration-tests.sh

integration-test-local:
	go test -tags integration -timeout 180s ./internal/integration/...

lint:
	golangci-lint run ./...

proto:
	buf generate

clean:
	go clean ./...

docker-build:
	docker build -t bunnymq:dev .

cluster-up:
	docker-compose up -d --build

cluster-down:
	docker-compose down -v

cluster-logs:
	docker-compose logs -f
