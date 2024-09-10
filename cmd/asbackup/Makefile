# Default values
# VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev") # uncomment this when move to separate repo.
VERSION := dev
COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := -ldflags "-X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(COMMIT)'"
OUTPUT := asbackup

# Build the application
build:
	@echo "Building asbackup with version $(VERSION)..."
	go build $(LDFLAGS) -o $(OUTPUT) main.go

# Clean the build
clean:
	rm -f $(OUTPUT)

# Build release locally
release-test:
	@echo "Testing release of asbackup with version $(VERSION)..."
	goreleaser build --snapshot

# Releasing
release:
	@echo "Releasing asbackup with version $(VERSION)..."
	goreleaser release
