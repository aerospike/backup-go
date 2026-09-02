# Backup Go
[![Tests](https://github.com/aerospike/backup-go/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/backup-go/actions/workflows/tests.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/backup-go)](https://pkg.go.dev/github.com/aerospike/backup-go)
[![codecov](https://codecov.io/gh/aerospike/backup-go/graph/badge.svg?token=S0gfl2zCcZ)](https://codecov.io/gh/aerospike/backup-go)

A Go library for backing up and restoring [Aerospike](https://aerospike.com/) data.

Backups are written in the ASB (Aerospike Backup) text format, optionally compressed and
encrypted, to a local directory or to S3, Google Cloud Storage or Azure Blob Storage.

## Official tools powered by this library

- [Aerospike Backup Service](https://github.com/aerospike/aerospike-backup-service)
- [Aerospike Backup CLI](https://github.com/aerospike/absctl)

## Install

```bash
go get github.com/aerospike/backup-go
```

Requires Go 1.25+ and [Aerospike Go client](https://github.com/aerospike/aerospike-client-go) v8.

## Stability

This module is at **v0.x and makes no API compatibility promise**. Exported names may change
in any release. Pin an exact version and read the release notes before upgrading.

## How it works

A `Client` wraps an Aerospike client and starts jobs. Every job is asynchronous: the call
returns a handler as soon as the job starts, and the handler is used to wait for the result
and to read stats and metrics. A `Client` is safe for concurrent use.

Three operations are available:

| Method | Purpose |
| --- | --- |
| `Client.Backup(ctx, cfg, writer, reader)` | Read records from a cluster and write a backup. The `reader` is only used to resume from a state file; pass `nil` otherwise. |
| `Client.Restore(ctx, cfg, reader)` | Read a backup and write the records back into a cluster. |
| `Client.Estimate(ctx, cfg, samples)` | Sample records to predict the backup size, writing nothing. |

The context given to `Backup` and `Restore` governs the job itself — canceling it stops the
job. The context given to `Wait` governs only the waiting, so it may be a different one.

## Backup

```go
asClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
if aerr != nil {
	log.Fatal(aerr)
}

backupClient, err := backup.NewClient(asClient)
if err != nil {
	log.Fatal(err)
}

ctx := context.Background()

// For a backup to a single file use options.WithFile(fileName).
writer, err := local.NewWriter(
	ctx,
	options.WithRemoveFiles(),
	options.WithDir("backups_folder"),
)
if err != nil {
	log.Fatal(err)
}

cfg := backup.NewDefaultBackupConfig()
cfg.Namespace = "source-ns" // defaults to "test", so set it explicitly
cfg.ParallelRead = 10
cfg.ParallelWrite = 10

// The last argument is a reader, needed only to resume from a state file.
handler, err := backupClient.Backup(ctx, cfg, writer, nil)
if err != nil {
	log.Fatal(err)
}

if err := handler.Wait(ctx); err != nil {
	log.Printf("Backup failed: %v", err)
}

stats := handler.GetStats()
```

## Restore

```go
// For a restore from a single file use options.WithFile(fileName).
reader, err := local.NewReader(
	ctx,
	options.WithDir("backups_folder"),
	options.WithValidator(asb.NewValidator()),
)
if err != nil {
	log.Fatal(err)
}

cfg := backup.NewDefaultRestoreConfig()
cfg.Parallel = 5

// Optional: restore into a different namespace.
source, dest := "source-ns", "dest-ns"
cfg.Namespace = &backup.RestoreNamespaceConfig{
	Source:      &source,
	Destination: &dest,
}

handler, err := backupClient.Restore(ctx, cfg, reader)
if err != nil {
	log.Fatal(err)
}

if err := handler.Wait(ctx); err != nil {
	log.Printf("Restore failed: %v", err)
}

stats := handler.GetStats()
```

A complete, compiling program that does both is in
[examples/readme/main.go](examples/readme/main.go). The `examples` directory also has
runnable programs for [S3](examples/aws/s3/main.go),
[GCP](examples/gcp/storage/main.go) and [Azure](examples/azure/blob/main.go).

## Storage backends

A destination is a `Writer`, a source is a `StreamingReader`. All backends take the shared
functional options from `io/storage/options`, so switching storage means swapping the
constructor.

| Import | Storage |
| --- | --- |
| `github.com/aerospike/backup-go/io/storage/local` | Local directory or file |
| `github.com/aerospike/backup-go/io/storage/aws/s3` | AWS S3 and S3-compatible |
| `github.com/aerospike/backup-go/io/storage/gcp/storage` | Google Cloud Storage |
| `github.com/aerospike/backup-go/io/storage/azure/blob` | Azure Blob Storage |
| `github.com/aerospike/backup-go/io/storage/std` | stdin / stdout |

## Supported imports

These import paths are what consumers are expected to use:

- `github.com/aerospike/backup-go` — clients, configs, handlers
- `github.com/aerospike/backup-go/models` — stats, metrics, retry policy
- `github.com/aerospike/backup-go/io/storage/...` — storage backends and their options
- `github.com/aerospike/backup-go/io/encoding/asb` — ASB format and its file validator
- `github.com/aerospike/backup-go/pkg/asinfo` — Aerospike info command client
- `github.com/aerospike/backup-go/pkg/secret-agent` — Secret Agent client

Packages under `pkg/server` are **under active development** and change without notice.

Everything else is a building block of the library rather than part of its supported
surface, and may change in any release — in particular `pipe`, `io/aerospike`,
`io/compression`, `io/encryption`, `io/counter`, `io/lazy`, `io/sized`, `pkg/estimates`, and
everything under `internal/`. Compression and encryption are configured through
`CompressionPolicy` and `EncryptionPolicy` on the config, not by importing those packages
directly.

## Configuration

Configs are plain structs created by `NewDefaultBackupConfig` and `NewDefaultRestoreConfig`,
then adjusted field by field. Every field is documented on
[pkg.go.dev](https://pkg.go.dev/github.com/aerospike/backup-go#ConfigBackup) — scope
(`Namespace`, `SetList`, `NodeList`, `RackList`, `PartitionFilters`), incremental filters
(`ModAfter`, `ModBefore`, `NoTTLOnly`), parallelism, rate limiting (`RecordsPerSecond` and
`Bandwidth`, raw bytes per second applied as given), output (`FileLimit`, `OutputFilePrefix`)
and resume (`StateFile` with `Continue`).

Two behaviours worth knowing before you start:

- `NewDefaultBackupConfig` sets `Namespace` to `"test"`, not to an empty string.
- `Backup`, `Restore` and `Estimate` fill a nil `ScanPolicy` / `WritePolicy` in **on the
  config you pass**, using a copy of the Aerospike client's default. That value stays on the
  config after the call, so use a fresh config per operation if that matters to you.

### Encryption and compression

`AES-128` / `AES-256`, with the key read from a file, an environment variable or the
[Secret Agent](https://aerospike.com/docs/tools/secret-agent), and ZSTD compression. The same
policies must be set on the restore config to read such a backup back.

```go
cfg.EncryptionPolicy = &backup.EncryptionPolicy{
	Mode:    backup.EncryptAES256,
	KeyFile: &keyFilePath,
}
cfg.CompressionPolicy = &backup.CompressionPolicy{
	Mode:  backup.CompressZSTD,
	Level: 3,
}
```

### Partition filters

```go
cfg.PartitionFilters = []*aerospike.PartitionFilter{
	backup.NewPartitionFilterByRange(0, 100),
	backup.NewPartitionFilterByID(200),
}

// The digest-based constructors also return an error.
afterDigest, err := backup.NewPartitionFilterAfterDigest("source-ns", "/+Ptyjj06wW9zx0AnxOmq45xJzs=")
if err != nil {
	log.Fatal(err)
}

cfg.PartitionFilters = append(cfg.PartitionFilters, afterDigest)
```

Each filter is a separate task that cannot be split further, so parallelism is capped by the
number of filters. Split ranges manually to get more.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build, test and mock generation.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
