.PHONY: test
test: test_deps
	go test -v ./...

.PHONY: coverage
coverage:
	go test ./... -coverprofile to_filter.cov -coverpkg ./... || true
	grep -v "test_resources\|mocks" to_filter.cov > coverage.cov
	rm to_filter.cov || true
	go tool cover -func coverage.cov

.PHONY: clean
clean:
	rm -f coverage.cov
	rm -rf mocks
	rm -rf pipeline/mocks

.PHONY: test_deps
test_deps: $(MOCKERY) generate

# Install mockery for generating test mocks
$(MOCKERY):
	go install github.com/vektra/mockery/v2@v2.42.0

.PHONY: generate
generate:
	go generate ./...
