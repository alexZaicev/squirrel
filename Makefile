GO        := go
GOFUMPT   := gofumpt
GCI       := gci
GOIMPORTS := goimports
GOLANGCI  := golangci-lint

.PHONY: fmt
fmt:
	$(GOFUMPT) -l -w .
	$(GCI) write -s Standard -s Default \
		-s 'Prefix(github.com)' \
		.
	$(GOIMPORTS) -local github.com/alexZaicev -w .

.PHONY: lint
lint:
	$(GOLANGCI) run --config .golangci.yaml ./...

.PHONY: unit
unit:
	$(GO) test -race -count=1 -cover -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | grep total: | awk '{print $$3}'

.PHONY: integration
integration:
	cd integration && $(GO) test -race -count=1 -v -args -driver sqlite3
	cd integration && $(GO) test -race -count=1 -v -args -driver mysql -dataSource 'root@tcp(localhost:3306)/squirrel'
	cd integration && $(GO) test -race -count=1 -v -args -driver postgres -dataSource 'postgres://postgres:postgres@localhost:5432/squirrel?sslmode=disable'
