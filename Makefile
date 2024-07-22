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
clean: mocks-clean
	rm -f coverage.cov

.PHONY: test_deps
test_deps: mocks-generate

# Install mockery for generating test mocks
.PHONY: mockery-install
mockery-install:
	go install github.com/vektra/mockery/v2@v2.42.0

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