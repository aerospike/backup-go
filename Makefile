SHELL = bash
WORKSPACE = $(shell pwd)
GO ?= $(shell which go || echo "/usr/local/go/bin/go")
NPROC := $(shell nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)

# Build tag guarding the tests that need Aerospike or object storage.
INTEGRATION_TAG = integration

GO_TEST_FLAGS = -parallel $(NPROC) -timeout=5m -count=1

GOVULNCHECK_VERSION = v1.7.0

.PHONY: fmt
fmt:
	golangci-lint fmt --build-tags=$(INTEGRATION_TAG)

.PHONY: lint
lint:
	golangci-lint run --build-tags=$(INTEGRATION_TAG)

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

# Unit tests only: no Aerospike, no object storage, no Docker.
.PHONY: test-unit
test-unit:
	$(GO) test $(GO_TEST_FLAGS) ./...

# Unit tests under the race detector. This is what CI runs on every PR.
.PHONY: test-race
test-race:
	$(GO) test -race $(GO_TEST_FLAGS) ./...

# Unit + integration tests. Needs the services listed in CONTRIBUTING.md.
.PHONY: test-integration
test-integration:
	$(GO) test -tags=$(INTEGRATION_TAG) $(GO_TEST_FLAGS) ./...

# Kept as the default entry point: hermetic, same as test-unit.
.PHONY: test
test:
	$(GO) test $(GO_TEST_FLAGS) -v ./...

.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

.PHONY: build-examples
build-examples:
	$(GO) build -o /dev/null ./examples/...

.PHONY: coverage
coverage:
	$(GO) test -tags=$(INTEGRATION_TAG) $(GO_TEST_FLAGS) ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	$(GO) tool cover -func coverage.cov

# Install mockery for generating test mocks. The version is pinned in go.mod
# through tools.go, so this always matches what CI checks against.
.PHONY: mockery-install
mockery-install:
	$(GO) install github.com/vektra/mockery/v3

# Iterate over project directory and generate mocks in packages where they must be.
# FYI: --recursively not working, because then mockery creates mock in root dirs, not putting them to /mocks folder.
.PHONY: mocks-generate
mocks-generate: mockery-install
	@echo "Generating mocks with config..."
	mockery --config=.mockery.yaml

# Removing all mocks in the project.
.PHONY: mocks-clean
mocks-clean:
	@echo "Cleaning up all 'mocks' directories..."
	@find . -type d -name 'mocks' -exec rm -rf {} +
