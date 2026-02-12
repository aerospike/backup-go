SHELL = bash
WORKSPACE = $(shell pwd)
GO ?= $(shell which go || echo "/usr/local/go/bin/go")
NPROC := $(shell nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)

.PHONY: test
test:
	go test -parallel $(NPROC) -timeout=5m -count=1 -v ./...

.PHONY: coverage
coverage:
	$(GO) test -parallel $(NPROC) -timeout=5m -count=1 ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	$(GO) tool cover -func coverage.cov

# Install mockery for generating test mocks.
.PHONY: mockery-install
mockery-install:
	$(GO) install github.com/vektra/mockery/v3@v3.6.3

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
