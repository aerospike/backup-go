.PHONY: test
test:
	go test -v ./...

.PHONY: coverage
coverage:
	go test ./... -coverprofile to_filter.cov -coverpkg ./... || true
	grep -v "test_resources\|mocks" to_filter.cov > coverage.cov
	rm to_filter.cov || true
	go tool cover -func coverage.cov

.PHONY: clean
clean:
	rm coverage.cov