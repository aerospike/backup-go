# Contributing

## Requirements

- Go 1.25+ (the exact version is in [go.mod](go.mod); CI reads it from there)
- Docker, for the services the integration tests need
- [Mockery](https://github.com/vektra/mockery) v3.6.3, installed by `make mockery-install`

## Make targets

| Target | What it does |
| --- | --- |
| `make test` | Runs the whole suite, `-count=1`, 5m timeout |
| `make coverage` | Same, with a coverage profile; this is what CI runs |
| `make mockery-install` | Installs the pinned mockery version |
| `make mocks-generate` | Regenerates all mocks from `.mockery.yaml` |
| `make mocks-clean` | Deletes every `mocks` directory |

## Tests

Unit tests run with no setup. The integration tests are not build-tagged — they are part of
the normal suite and fail without their services running, so `make test` on a bare machine
will report failures for the storage and Aerospike packages.

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

The Aerospike image is **Enterprise Edition**. It runs under the
[Aerospike evaluation licence](https://aerospike.com/legal/eval-license/), which allows
development and testing use on a single node. It is required because the suite covers
features that Community Edition does not have.

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

Every `.go` file starts with the Apache 2.0 header; the `license-checker` workflow enforces
it. Copy the header from any existing file into new ones.

## Pull requests

Target the `dev` branch. `main` holds releases. Both CI suites — tests and lint — must be
green, and generated mocks must be up to date.
