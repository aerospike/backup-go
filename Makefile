.PHONY: test
test: test_deps
	go test -v ./...

.PHONY: coverage
coverage: test_deps
	go test ./... -coverprofile to_filter.cov -coverpkg ./...
	grep -v "test\|mocks" to_filter.cov > coverage.cov
	rm -f to_filter.cov
	go tool cover -func coverage.cov

.PHONY: clean
clean:
	rm -f coverage.cov
	rm -rf mocks
	rm -rf pipeline/mocks
	rm -f mock_statsSetterToken.go
	rm -f mock_statsSetterExpired.go
	rm -rf encoding/mocks
	rm -rf internal/asinfo/mocks

.PHONY: test_deps
test_deps: mocks-generate

# Install mockery for generating test mocks
.PHONY: mockery-install
mockery-install:
	go install github.com/vektra/mockery/v2@v2.42.0

# Iterate over project directory and generate mocks in packages where they must be.
# FYI: --recursively not working, because then mockery creates mock in root dirs, not putting them to /mocks folder.
.PHONY: mocks-generate
GO_SRC_DIRS := $(shell find . -type f -name '*.go' -exec dirname {} \; | sort -u)
mocks-generate: mockery-install
	@for dir in $(GO_SRC_DIRS); do \
		echo "Checking directory $$dir"; \
		goFiles=$$(find $$dir -maxdepth 1 -name '*.go'); \
		if [ -n "$$goFiles" ]; then \
			echo "Generating mocks in $$dir"; \
			mockery --dir=$$dir --output=$$dir/mocks --outpkg=mocks --exported --with-expecter --all; \
		else \
			echo "No .go files in $$dir, skipping"; \
		fi; \
	done

# Removing all mocks in the project.
.PHONY: mocks-clean
mocks-clean:
	@echo "Cleaning up all 'mocks' directories..."
	@find . -type d -name 'mocks' -exec rm -rf {} +