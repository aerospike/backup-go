VERSION := dev
COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := -ldflags "-X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(COMMIT)'"

NPROC = $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)

.PHONY: test
test:
	go test -parallel $(NPROC) -timeout=5m -count=1 -v ./...

.PHONY: coverage
coverage:
	go test -parallel $(NPROC) -timeout=5m -count=1 ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	go tool cover -func coverage.cov

.PHONY: clean
clean: mocks-clean
	rm -f coverage.cov
	rm -Rf dist

# Install mockery for generating test mocks.
.PHONY: mockery-install
mockery-install:
	go install github.com/vektra/mockery/v3@v3.2.5

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

# Build release locally.
.PHONY: release-test
release-test:
	@echo "Testing release with version $(VERSION)..."
	goreleaser build --snapshot


# Build CLI tools.
.PHONY: build
build:
	mkdir dist
	@echo "Building asbackup with version $(VERSION)..."
	go build -o dist/asbackup cmd/asbackup/main.go
	@echo "Building asrestore with version $(VERSION)..."
	go build -o dist/asrestore cmd/asrestore/main.go
