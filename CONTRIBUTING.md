# Contributing

## Requirements

- Go 1.25+ (the language version and toolchain are in [go.mod](go.mod); CI reads them from there)
- Docker, only for the integration tests
- [Mockery](https://github.com/vektra/mockery), pinned in [tools.go](tools.go) / [go.mod](go.mod)
  and installed by `make mockery-install`

## Make targets

| Target | What it does |
| --- | --- |
| `make test-unit` | Unit tests only. No Aerospike, no object storage, no Docker |
| `make test-race` | The same tests under the race detector; CI runs this on every PR |
| `make test-integration` | Unit **and** integration tests. Needs the services below |
| `make test` | Verbose alias for the unit tests |
| `make coverage` | Unit + integration tests with a coverage profile; this is what CI uploads |
| `make fmt` | Applies the formatters from `.golangci.yaml` |
| `make lint` | Runs golangci-lint |
| `make vet` | Runs `go vet ./...` |
| `make tidy` | Runs `go mod tidy`; CI fails a PR that leaves `go.mod`/`go.sum` untidy |
| `make vuln` | Runs govulncheck; CI requires it to pass |
| `make build-examples` | Compiles everything under `./examples/...` |
| `make mockery-install` | Installs the pinned mockery version |
| `make mocks-generate` | Regenerates all mocks from `.mockery.yaml` |
| `make mocks-clean` | Deletes every `mocks` directory |

## Tests

Tests that need Aerospike or object storage are behind the `integration` build tag, so a
fresh clone with no Docker running passes:

```bash
make test-unit
```

`make test-integration` adds the tagged tests and needs the services below. New tests that
talk to a real service belong in a `*_integration_test.go` file starting with:

```go
//go:build integration
```

The AWS tests write their MinIO profile into the test's own `t.TempDir()` and point the SDK
at it, so nothing ever touches `~/.aws/credentials`.

To run everything you need, matching
[.github/workflows/tests.yml](.github/workflows/tests.yml):

```bash
docker run -d -p 3000-3002:3000-3002 aerospike/aerospike-server-enterprise:8.0.0.7
```

```bash
docker run -d -p 10000:10000 --name azurite mcr.microsoft.com/azure-storage/azurite:3.35.0 azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
```

```bash
docker run -d -p 4443:4443 --name fake-gcs --entrypoint sh fsouza/fake-gcs-server:1.52.2 -c "/bin/fake-gcs-server -data /data -scheme http -public-host 127.0.0.1:4443"
```

MinIO stands in for S3 and needs two buckets, `backup` and `asbackup`; see the `Set up Minio`
step in the workflow for the exact `mc` commands.

### Community Edition vs Enterprise Edition

The Aerospike image is **Enterprise Edition**. It runs under the
[Aerospike evaluation licence](https://aerospike.com/legal/eval-license/), which allows
development and testing use on a single node.

Enterprise is required, not a preference: the integration suite exercises features that
Community Edition does not have — among them rack awareness, the `racks:` info command and
rack-filtered backups, and server-side backup and restore jobs. Those tests fail against a CE
node, so no CE image is offered here. Anything you can check without those features is
already covered by `make test-unit`, which needs no server at all.

## Mocks

Mocks are generated, committed, and checked in CI: the `mocks-check` workflow regenerates
them and fails on any diff. After changing an interface:

```bash
make mocks-generate
```

Then commit the result together with the interface change.

## Linting

CI runs golangci-lint v2.12.2 against [.golangci.yaml](.golangci.yaml). The repo also has a
pre-commit hook for it, plus gitleaks, end-of-file and trailing-whitespace fixers:

```bash
pre-commit install
```

A local golangci-lint older than v2 will reject the config with "unknown linters" — match the
CI version.

## License headers

Every `.go` file starts with the Apache 2.0 header, test files and `tests/` included; the
`license-checker` workflow enforces it. Copy the header from any existing file into new ones.

## Pull requests

Target the `dev` branch. `main` holds releases. Every CI gate must be green: unit tests under
the race detector, integration tests, lint, license headers, up-to-date mocks, a tidy
`go.mod`/`go.sum`, compiling examples, and the vulnerability scans (Snyk fails on High and
Critical; govulncheck must report nothing).
