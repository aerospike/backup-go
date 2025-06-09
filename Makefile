SHELL = bash
WORKSPACE = $(shell pwd)
VERSION := $(shell cat VERSION)
MAINTAINER = "Aerospike <info@aerospike.com>"
DESCRIPTION = "Aerospike Backup Service"
HOMEPAGE = "https://www.aerospike.com"
VENDOR = "Aerospike INC"
LICENSE = "Apache License 2.0"

# Go parameters
GO ?= $(shell which go || echo "/usr/local/go/bin/go")
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
REGISTRY ?= "docker.io"
RH_REGISTRY ?= "registry.access.redhat.com"
GIT_COMMIT:=$(shell git rev-parse HEAD)
GOBUILD = GOOS=$(OS) GOARCH=$(ARCH) $(GO) build \
-ldflags="-X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(GIT_COMMIT)' -X 'main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')'"
GOTEST = $(GO) test
GOCLEAN = $(GO) clean
GOBIN_VERSION = $(shell $(GO) version 2>/dev/null)
NPROC := $(shell nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)


BACKUP_BINARY_NAME = asbackup
RESTORE_BINARY_NAME = asrestore
TARGET_DIR = $(WORKSPACE)/target
CMD_BACKUP_DIR = $(WORKSPACE)/cmd/$(BACKUP_BINARY_NAME)
CMD_RESTORE_DIR = $(WORKSPACE)/cmd/$(RESTORE_BINARY_NAME)


.PHONY: test
test:
	go test -parallel $(NPROC) -timeout=5m -count=1 -v ./...

.PHONY: coverage
coverage:
	$(GO) test -parallel $(NPROC) -timeout=5m -count=1 ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	$(GO) tool cover -func coverage.cov

.PHONY: clean
clean:
	$(GO) clean
	rm -Rf $(TARGET_DIR)

# Install mockery for generating test mocks.
.PHONY: mockery-install
mockery-install:
	$(GO) install github.com/vektra/mockery/v3@v3.2.5

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

.PHONY: docker-build
docker-build:
	 DOCKER_BUILDKIT=1  docker build --progress=plain --tag aerospike/aerospike-backup-tools:$(TAG) --build-arg REGISTRY=$(REGISTRY) --build-arg RH_REGISTRY=$(RH_REGISTRY) --file $(WORKSPACE)/Dockerfile .

.PHONY: docker-buildx
docker-buildx:
	./scripts/docker-buildx.sh --tag $(TAG) --registry $(REGISTRY) --rh-registry $(RH_REGISTRY)

# Build CLI tools.
.PHONY: build
build:
	mkdir -p "$(TARGET_DIR)"
	$(GOBUILD) -o $(TARGET_DIR)/$(BACKUP_BINARY_NAME)_$(OS)_$(ARCH) $(CMD_BACKUP_DIR)
	$(GOBUILD) -o $(TARGET_DIR)/$(RESTORE_BINARY_NAME)_$(OS)_$(ARCH) $(CMD_RESTORE_DIR)

