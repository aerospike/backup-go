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
test_deps: mocks

.PHONY: mocks
mocks: mockery
	go generate ./...

# Install mockery for generating test mocks
.PHONY: mockery
mockery:
	go install github.com/vektra/mockery/v2@v2.42.0
