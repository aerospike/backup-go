SHELL = bash
NAME = aerospike-backup-tools
WORKSPACE = $(shell pwd)
VERSION ?= $(shell cat VERSION)
MAINTAINER = "Aerospike <info@aerospike.com>"
DESCRIPTION = "Aerospike Backup Tools"
HOMEPAGE = "https://www.aerospike.com"
VENDOR = "Aerospike INC"
LICENSE = "Apache License 2.0"

# Go parameters
GO ?= $(shell which go || echo "/usr/local/go/bin/go")
NFPM ?= $(shell which nfpm)
OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
REGISTRY ?= "docker.io"
GIT_COMMIT:=$(shell git rev-parse HEAD)
GOBUILD = GOOS=$(OS) GOARCH=$(ARCH) CGO_ENABLED=0 $(GO) build \
-ldflags="-s -w -X 'main.appVersion=$(VERSION)' -X 'main.commitHash=$(GIT_COMMIT)' -X 'main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')'"
GOTEST = $(GO) test
GOCLEAN = $(GO) clean
GOBIN_VERSION = $(shell $(GO) version 2>/dev/null)
NPROC := $(shell nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)

ARCHS ?= linux/amd64 linux/arm64
PACKAGERS ?= deb rpm

IMAGE_TAG ?= test
IMAGE_REPO ?= aerospike/aerospike-backup-tools

BACKUP_BINARY_NAME = asbackup
RESTORE_BINARY_NAME = asrestore
TARGET_DIR = $(WORKSPACE)/target
PACKAGE_DIR= $(WORKSPACE)/scripts/package
CMD_BACKUP_DIR = $(WORKSPACE)/cmd/$(BACKUP_BINARY_NAME)
CMD_RESTORE_DIR = $(WORKSPACE)/cmd/$(RESTORE_BINARY_NAME)
INSTALL_DIR ?= /usr/bin


.PHONY: test
test:
	go test -parallel $(NPROC) -timeout=5m -count=1 -v ./...

.PHONY: coverage
coverage:
	$(GO) test -parallel $(NPROC) -race -timeout=5m -count=1 ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	$(GO) tool cover -func coverage.cov

.PHONY: clean
clean:
	rm -Rf $(TARGET_DIR)
	@find . -type f -name 'nfpm-linux-*.yaml' -exec rm -v {} +


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
	 DOCKER_BUILDKIT=1 docker build \
 	--progress=plain \
 	--tag $(IMAGE_REPO):$(IMAGE_TAG) \
 	--build-arg REGISTRY=$(REGISTRY) \
 	--file $(WORKSPACE)/Dockerfile .

.PHONY: docker-buildx
docker-buildx:
	./scripts/docker-buildx.sh --tag $(IMAGE_TAG) --registry $(REGISTRY) --platforms "$(ARCHS)"

.PHONY: build
build:
	mkdir -p "$(TARGET_DIR)"
	@echo "Building $(BACKUP_BINARY_NAME) with version $(VERSION)..."
	$(GOBUILD) -o $(TARGET_DIR)/$(BACKUP_BINARY_NAME)_$(OS)_$(ARCH) $(CMD_BACKUP_DIR)
	@echo "Building $(RESTORE_BINARY_NAME) with version $(VERSION)..."
	$(GOBUILD) -o $(TARGET_DIR)/$(RESTORE_BINARY_NAME)_$(OS)_$(ARCH) $(CMD_RESTORE_DIR)

.PHONY: buildx
buildx:
	@for arch in $(ARCHS); do \
  		OS=$$(echo $$arch | cut -d/ -f1); \
  		ARCH=$$(echo $$arch | cut -d/ -f2); \
  		OS=$$OS ARCH=$$ARCH $(MAKE) build; \
  	done

.PHONY: packages
packages: buildx
	@for arch in $(ARCHS); do \
  		OS=$$(echo $$arch | cut -d/ -f1); \
  		ARCH=$$(echo $$arch | cut -d/ -f2); \
		OS=$$OS ARCH=$$ARCH \
		NAME=$(NAME) \
		VERSION=$(VERSION) \
		WORKSPACE=$(WORKSPACE) \
		MAINTAINER=$(MAINTAINER) \
		DESCRIPTION=$(DESCRIPTION) \
		HOMEPAGE=$(HOMEPAGE) \
		VENDOR=$(VENDOR) \
		LICENSE=$(LICENSE) \
		BACKUP_BINARY_NAME=$(BACKUP_BINARY_NAME) \
		RESTORE_BINARY_NAME=$(RESTORE_BINARY_NAME) \
		envsubst '$$OS $$ARCH $$NAME $$VERSION $$WORKSPACE $$MAINTAINER $$DESCRIPTION $$HOMEPAGE $$VENDOR $$LICENSE $$BACKUP_BINARY_NAME $$RESTORE_BINARY_NAME' \
		< $(PACKAGE_DIR)/nfpm.tmpl.yaml > $(PACKAGE_DIR)/nfpm-$$OS-$$ARCH.yaml; \
		for packager in $(PACKAGERS); do \
			$(NFPM) package \
			--config $(PACKAGE_DIR)/nfpm-$$OS-$$ARCH.yaml \
			--packager $$(echo $$packager) \
			--target $(TARGET_DIR); \
			done; \
  	done; \

.PHONY: checksums
checksums:
	@find . -type f \
		\( -name '*.deb' -o -name '*.rpm' \) \
		-exec sh -c 'sha256sum "$$1" | cut -d" " -f1 > "$$1.sha256"' _ {} \;

.PHONY: install
install:
	install -m 0755 $(TARGET_DIR)/$(BACKUP_BINARY_NAME)_$(OS)_$(ARCH) $(INSTALL_DIR)/$(BACKUP_BINARY_NAME)
	install -m 0755 $(TARGET_DIR)/$(RESTORE_BINARY_NAME)_$(OS)_$(ARCH) $(INSTALL_DIR)/$(RESTORE_BINARY_NAME)

.PHONY: uninstall
uninstall:
	@rm -f $(INSTALL_DIR)/$(BACKUP_BINARY_NAME)
	@rm -f $(INSTALL_DIR)/$(RESTORE_BINARY_NAME)

.PHONY: vulnerability-scan
vulnerability-scan:
	snyk test --all-projects --policy-path=$(WORKSPACE)/.snyk --severity-threshold=high

.PHONY: vulnerability-scan-container
vulnerability-scan-container:
	snyk container test $(IMAGE_REPO):$(IMAGE_TAG) \
	--policy-path=$(WORKSPACE)/.snyk \
	--file=Dockerfile \
	--severity-threshold=high
