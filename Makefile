# Copyright (c) 2025 Visvasity LLC

export GO ?= $(shell which go)
export GOBIN = $(CURDIR)
# export PATH = $(CURDIR):$(HOME)/bin:$(HOME)/go/bin:/bin:/usr/bin:/usr/local/bin:/sbin:/usr/sbin
export GOTESTFLAGS ?=

NO_DIRTY_VERSION = $(shell git describe --always --tags)
DIRTY_VERSION = $(shell git describe --dirty --always --tags)

.PHONY: all
all: go-all go-test go-test-long;

.PHONY: clean
clean:
	git clean -f -X

.PHONY: bash
bash:
	@echo \#
	@echo \# Interactive BASH SHELL with the build environment
	@echo \#
	bash -li

.PHONY: go-generate
go-generate:
	$(GO) generate ./...

.PHONY: go-all
go-all: go-generate
	$(GO) build ./...

.PHONY: go-test
go-test: go-all
	$(GO) test -fullpath -count=1 -coverprofile=coverage.out -short $(GOTESTFLAGS) ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

.PHONY: go-test-long
go-test-long: go-all
	$(GO) test -fullpath -failfast -count=1 -coverprofile=coverage.out $(GOTESTFLAGS) ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

# .PHONY: go-bench
# go-bench: go-all
# 	$(GO) test -count=10 -benchmem -bench=BenchmarkTest -run=^$$ $(GOBENCHFLAGS) ./... | tee ./benchmarks/benchmark-$(DIRTY_VERSION).txt
# 	test ! -f ./benchmarks/baseline.txt || benchstat ./benchmarks/baseline.txt ./benchmarks/benchmark-$(DIRTY_VERSION).txt

# Include developer's custom make rules if exists.
-include $(CURDIR)/../Makefile.custom
