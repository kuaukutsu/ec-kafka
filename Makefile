GO_VERSION = "1.27.3"
GOLANGCI_LINT_VERSION = "v2.14.0"
GOFUMPT_VERSION = "v0.11.0"
GOBIN = "$$(pwd)/.bin"
USER = $$(id -u)
ARGS = $(filter-out $@,$(MAKECMDGOALS))

# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help build run test race pprof bench
.DEFAULT_GOAL := help

help: ## Display this help screen
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# Спец-правило, чтобы Makefile не ругался на неизвестные команды (аргументы)
%:
	@:
	
## Claude

-include .claude/Makefile

## Claude

-include .codex/Makefile

## App
	
goset:
	asdf set golang $(GO_VERSION)

lint: _golangci_lint_install ## golangci-lint
	$(GOBIN)/golangci-lint run ./... --config=.golangci.yml --fix

lint-check: _golangci_lint_install
	$(GOBIN)/golangci-lint run ./... --config=.golangci.yml

fmt: _gofumpt_install ## gofumpt
	$(GOBIN)/gofumpt -extra -l -w .

fmt-check: _gofumpt_install
	$(GOBIN)/gofumpt -extra -l .

run:
	go run -tags local cmd/$(ARGS)/main.go --config=./config/local.yaml

cevent:
	KAFKA_CONSUMER_GROUP=env KAFKA_CONSUMER_TOPIC=playground.events \
	    go run -tags local cmd/consumer/main.go --config=./config/local.yaml
	
race:
	go run -race ./$(ARGS) -run='^$$'

tests: ### run unit-test
	- go clean -testcache
	- go test ./internal/...
	
_golangci_lint_install:
	[ -f $(GOBIN)/golangci-lint ] || GOBIN=$(GOBIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

_gofumpt_install:
	[ -f $(GOBIN)/gofumpt ] || GOBIN=$(GOBIN) go install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)
