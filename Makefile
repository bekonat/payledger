TEST_DATABASE_URL ?= postgres://payledger:payledger@localhost:5432/payledger?sslmode=disable

.PHONY: build test test-integration cover lint proto up down e2e load

build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test -race -count=1 -tags integration ./internal/postgres/...

cover:
	go test -coverprofile=coverage.out ./internal/...
	go tool cover -func=coverage.out | tail -1

lint:
	go vet ./... && go vet -tags integration ./...
	test -z "$$(gofmt -l .)"

proto:
	cd gen && go generate ./...

up:
	docker compose up --build -d

down:
	docker compose down -v

e2e:
	./scripts/e2e.sh

load:
	go run ./cmd/loadtest -addr http://localhost:8080 -accounts 100 -c 32 -d 15s
