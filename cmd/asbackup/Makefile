# Default values
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