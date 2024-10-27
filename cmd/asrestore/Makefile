# Default values
# VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev") # uncomment this when move to separate repo.
VERSION := dev
COMMIT := $(shell git rev-parse --short HEAD)
LDFLAGS := -ldflags "-X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(COMMIT)'"
OUTPUT := asrestore

# Build the application
build:
	@echo "Building $(OUTPUT) with version $(VERSION)..."
	go build $(LDFLAGS) -o $(OUTPUT) main.go

# Clean the build
clean:
	rm -f $(OUTPUT)
	rm -Rf dist
