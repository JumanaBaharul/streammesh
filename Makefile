BINARY  := streammesh
PKG     := ./...
GOFLAGS ?=

.PHONY: all build run test test-race cover bench lint fmt vet fuzz demo clean docker tidy

all: fmt vet test-race build

build:
	go build $(GOFLAGS) -o bin/$(BINARY) ./cmd/streammesh

run:
	go run ./cmd/streammesh -config examples/pipelines.yaml

test:
	go test $(GOFLAGS) $(PKG)

test-race:
	go test $(GOFLAGS) -race -count=1 $(PKG)

cover:
	go test -race -covermode=atomic -coverprofile=coverage.txt $(PKG)
	go tool cover -func=coverage.txt | tail -1

bench:
	go test -run '^$$' -bench . -benchmem ./internal/...

vet:
	go vet $(PKG)

lint:
	gofmt -l . && go vet $(PKG)

fmt:
	gofmt -w .

fuzz:
	go test -run '^$$' -fuzz FuzzSyslog -fuzztime 20s ./internal/parse

demo:
	docker compose up --build

docker:
	docker build -t streammesh:dev .

tidy:
	go mod tidy

clean:
	rm -rf bin dist coverage.txt coverage.html .streammesh out
