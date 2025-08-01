# Backup Go
[![Tests](https://github.com/aerospike/backup-go/actions/workflows/tests.yml/badge.svg)](https://github.com/aerospike/backup-go/actions/workflows/tests.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/aerospike/backup-go)](https://pkg.go.dev/github.com/aerospike/backup-go)
[![codecov](https://codecov.io/gh/aerospike/backup-go/graph/badge.svg?token=S0gfl2zCcZ)](https://codecov.io/gh/aerospike/backup-go)

A Go library for backing up and restoring [Aerospike](https://aerospike.com/) data, with support for both standard and
transactionally consistent backups.

## Official tools powered by this library
- [Aerospike Backup Service](https://github.com/aerospike/aerospike-backup-service)
- [Aerospike Backup CLI](https://github.com/aerospike/aerospike-backup-cli)

## Features

- Standard backup and restore operations
- Flexible backup configurations including:
  - Partition-based backups
  - Node-based backups
  - Incremental backups using modification time filters
  - Compression (ZSTD)
  - Encryption (AES-128/256)
  - [Secret Agent](https://aerospike.com/docs/tools/secret-agent) integration
- Multiple backup formats:
  - ASB (Aerospike Backup) text format
- Configurable parallelism for both reading and writing
- Support for backup file size limits and state preservation
- Bandwidth and records-per-second rate limiting

## Design

This Aerospike backup package is built around the [Aerospike Go client](https://github.com/aerospike/aerospike-client-go).
The package uses a client structure to start backup and restore jobs. The client structure is thread safe,
backup and restore jobs can be started in multiple goroutines. When the client is used to start backup and restore
jobs, a handler is immediately returned that is used to check the job's status, errors, and wait for it to finish.

### Key Components

- **Client**: The main entry point for backup operations
- **Writers/Readers**: Handle backup data I/O
- **Configurations**: Define backup/restore behavior

## Usage

### Standard Backup

The regular backup operation backs up data from an Aerospike database based on a user-defined
configuration. First, a scan operation uses the configured scope to query the database and
retrieve matching records. Then, a decoder converts the retrieved data into the `asb` format,
which is subsequently stored by a supported writer.

```go
package main

import (
  "context"
  "log"

  "github.com/aerospike/aerospike-client-go/v8"
  "github.com/aerospike/backup-go"
  ioStorage "github.com/aerospike/backup-go/io/storage"
  "github.com/aerospike/backup-go/io/storage/local"
)

func main() {
  // Create Aerospike client.
  aerospikeClient, aerr := aerospike.NewClient("127.0.0.1", 3000)
  if aerr != nil {
    log.Fatal(aerr)
  }

  // Create backup client.
  backupClient, err := backup.NewClient(aerospikeClient, backup.WithID("client_id"))
  if err != nil {
    log.Fatal(err)
  }

  ctx := context.Background()

  // Configure writers for backup.
  // For backup to single file use local.WithFile(fileName).
  writers, err := local.NewWriter(
    ctx,
    ioStorage.WithRemoveFiles(),
    ioStorage.WithDir("backups_folder"),
  )
  if err != nil {
    log.Fatal(err)
  }

  // Configure backup.
  backupCfg := backup.NewDefaultBackupConfig()
  backupCfg.Namespace = "test"
  backupCfg.ParallelRead = 10
  backupCfg.ParallelWrite = 10

  // Start backup.
  backupHandler, err := backupClient.Backup(ctx, backupCfg, writers, nil)
  if err != nil {
    log.Fatal(err)
  }

  // Wait for completion. 
  // Use backupHandler.Wait(ctx) to wait for the job to finish or fail.
  // You can use different context here, and if it is canceled
  // backupClient.Backup(ctx, backupCfg, writers) context will be cancelled too.
  if err = backupHandler.Wait(ctx); err != nil {
    log.Printf("Backup failed: %v", err)
  }
}
```

### Restore

The restore operation reads backup files in both `asb` and `asbx` formats and restores them using
the configured backup client.

```go
func main() {
    // ... create clients as above ...

    // Configure restore
    restoreCfg := backup.NewDefaultRestoreConfig()
    restoreCfg.Parallel = 5
    
    // Optional: configure namespace mapping
    source := "source-ns"
    dest := "dest-ns"
    restoreCfg.Namespace = &backup.RestoreNamespaceConfig{
        Source:      &source,
        Destination: &dest,
    }

    // Create reader for restore
    reader, err := local.NewReader(
        ioStorage.WithValidator(asb.NewValidator()),
        ioStorage.WithDir("backups_folder"),
    )
    if err != nil {
        panic(err)
    }

    // Start restore
    restoreHandler, err := backupClient.Restore(ctx, restoreCfg, reader)
    if err != nil {
        panic(err)
    }

    // Wait for completion
    if err = restoreHandler.Wait(ctx); err != nil {
        log.Printf("Restore failed: %v", err)
    }

    // Check restore statistics
    stats := restoreHandler.GetStats()
}
```
## Configuration Options

<details>
<summary>Backup Configuration</summary>

### Backup Configuration

```go
type ConfigBackup struct {
    // InfoPolicy applies to Aerospike Info requests made during backup and
    // restore. If nil, the Aerospike client's default policy will be used.
    InfoPolicy *a.InfoPolicy
    // ScanPolicy applies to Aerospike scan operations made during backup and
    // restore. If nil, the Aerospike client's default policy will be used.
    ScanPolicy *a.ScanPolicy
    // Only include records that last changed before the given time (optional).
    ModBefore *time.Time
    // Only include records that last changed after the given time (optional).
    ModAfter *time.Time
    // Encryption details.
    EncryptionPolicy *EncryptionPolicy
    // Compression details.
    CompressionPolicy *CompressionPolicy
    // Secret agent config.
    SecretAgentConfig *SecretAgentConfig
    // PartitionFilters specifies the Aerospike partitions to back up.
    // Partition filters can be ranges, individual partitions,
    // or records after a specific digest within a single partition.
    // Note:
    // if not default partition filter NewPartitionFilterAll() is used,
    // each partition filter is an individual task which cannot be parallelized,
    // so you can only achieve as much parallelism as there are partition filters.
    // You may increase parallelism by dividing up partition ranges manually.
    // AfterDigest:
    // afterDigest filter can be applied with
    // NewPartitionFilterAfterDigest(namespace, digest string) (*a.PartitionFilter, error)
    // Backup records after record digest in record's partition plus all succeeding partitions.
    // Used to resume backup with last record received from previous incomplete backup.
    // This parameter will overwrite PartitionFilters.Begin value.
    // Can't be used in full backup mode.
    // This parameter is mutually exclusive with partition-list (not implemented).
    // Format: base64 encoded string.
    // Example: EjRWeJq83vEjRRI0VniavN7xI0U=
    PartitionFilters []*a.PartitionFilter
    // Namespace is the Aerospike namespace to back up.
    Namespace string
    // NodeList contains a list of nodes to back up.
    // <IP addr 1>:<port 1>[,<IP addr 2>:<port 2>[,...]]
    // <IP addr 1>:<TLS_NAME 1>:<port 1>[,<IP addr 2>:<TLS_NAME 2>:<port 2>[,...]]
    // Backup the given cluster nodes only.
    // If it is set, ParallelNodes automatically set to true.
    // This argument is mutually exclusive with partition-list/AfterDigest arguments.
    NodeList []string
    // SetList is the Aerospike set to back up (optional, given an empty list,
    // all sets will be backed up).
    SetList []string
    // The list of backup bin names
    // (optional, given an empty list, all bins will be backed up)
    BinList []string
    // ParallelNodes specifies how to perform scan.
    // If set to true, we launch parallel workers for nodes; otherwise workers run in parallel for partitions.
    // Excludes PartitionFilters param.
    ParallelNodes bool
    // EncoderType describes an Encoder type that will be used on backing up.
    // Default `EncoderTypeASB` = 0.
    EncoderType EncoderType
    // ParallelRead is the number of concurrent scans to run against the Aerospike cluster.
    ParallelRead int
    // ParallelWrite is the number of concurrent backup files writing.
    ParallelWrite int
    // Don't back up any records.
    NoRecords bool
    // Don't back up any secondary indexes.
    NoIndexes bool
    // Don't back up any UDFs.
    NoUDFs bool
    // RecordsPerSecond limits backup records per second (rps) rate.
    // Will not apply rps limit if RecordsPerSecond is zero (default).
    RecordsPerSecond int
    // Limits backup bandwidth (bytes per second).
    // Will not apply rps limit if Bandwidth is zero (default).
    Bandwidth int
    // File size limit (in bytes) for the backup. If a backup file exceeds this
    // size threshold, a new file will be created. 0 for no file size limit.
    FileLimit int64
    // Do not apply base-64 encoding to BLOBs: Bytes, HLL, RawMap, RawList.
    // Results in smaller backup files.
    Compact bool
    // Only include records that have no ttl set (persistent records).
    NoTTLOnly bool
    // Name of a state file that will be saved in backup directory.
    // Works only with FileLimit parameter.
    // As we reach FileLimit and close file, the current state will be saved.
    // Works only for default and/or partition backup.
    // Not work with ParallelNodes or NodeList.
    StateFile string
    // Resumes an interrupted/failed backup from where it was left off, given the .state file
    // that was generated from the interrupted/failed run.
    // Works only for default and/or partition backup. Not work with ParallelNodes or NodeList.
    Continue bool
    // How many records will be read on one iteration for continuation backup.
    // Affects size if overlap on resuming backup after an error.
    // By default, it must be zero. If any value is set, reading from Aerospike will be paginated.
    // Which affects the performance and RAM usage.
    PageSize int64
    // If set to true, the same number of workers will be created for each stage of the pipeline.
    // Each worker will be connected to the next stage worker with a separate unbuffered channel.
    PipelinesMode pipeline.Mode
    // When using directory parameter, prepend a prefix to the names of the generated files.
    OutputFilePrefix string
    // Retry policy for info commands.
    InfoRetryPolicy *models.RetryPolicy
}
```
</details>

<details>
<summary>Restore Configuration</summary>

### Restore Configuration

```go
type ConfigRestore struct {
    // InfoPolicy applies to Aerospike Info requests made during backup and restore
    // If nil, the Aerospike client's default policy will be used.
    InfoPolicy *a.InfoPolicy
    // WritePolicy applies to Aerospike write operations made during backup and restore
    // If nil, the Aerospike client's default policy will be used.
    WritePolicy *a.WritePolicy
    // Namespace details for the restore operation.
    // By default, the data is restored to the namespace from which it was taken.
    Namespace *RestoreNamespaceConfig `json:"namespace,omitempty"`
    // Encryption details.
    EncryptionPolicy *EncryptionPolicy
    // Compression details.
    CompressionPolicy *CompressionPolicy
    // Configuration of retries for each restore write operation.
    // If nil, no retries will be performed.
    RetryPolicy *models.RetryPolicy
    // Secret agent config.
    SecretAgentConfig *SecretAgentConfig
    // The sets to restore (optional, given an empty list, all sets will be restored).
    SetList []string
    // The bins to restore (optional, given an empty list, all bins will be restored).
    BinList []string
    // EncoderType describes an Encoder type that will be used on restoring.
    // Default `EncoderTypeASB` = 0.
    EncoderType EncoderType
    // Parallel is the number of concurrent record readers from backup files.
    Parallel int
    // RecordsPerSecond limits restore records per second (rps) rate.
    // Will not apply rps limit if RecordsPerSecond is zero (default).
    RecordsPerSecond int
    // Limits restore bandwidth (bytes per second).
    // Will not apply rps limit if Bandwidth is zero (default).
    Bandwidth int
    // Don't restore any records.
    NoRecords bool
    // Don't restore any secondary indexes.
    NoIndexes bool
    // Don't restore any UDFs.
    NoUDFs bool
    // Disables the use of batch writes when restoring records to the Aerospike cluster.
    DisableBatchWrites bool
    // The max allowed number of records per batch write call.
    BatchSize int
    // Max number of parallel writers to target AS cluster.
    MaxAsyncBatches int
    // Amount of extra time-to-live to add to records that have expirable void-times.
    // Must be set in seconds.
    ExtraTTL int64
    // Ignore permanent record-specific error.
    // E.g.: AEROSPIKE_RECORD_TOO_BIG.
    // By default, such errors are not ignored and restore terminates.
    IgnoreRecordError bool
    // Retry policy for info commands.
    InfoRetryPolicy *models.RetryPolicy
}
```
</details>

## Advanced Features

### Encryption

The library supports `AES-128` and `AES-256` encryption with keys from:
- Files
- Environment variables
- Aerospike Secret Agent

```go
// For backup encryption.
backupCfg.EncryptionPolicy = &backup.EncryptionPolicy{
    Mode:     backup.EncryptAES256,
    KeyFile:  &keyFilePath,
}
```

```go
// For restore encrypted backup.
restoreCfg.EncryptionPolicy = &backup.EncryptionPolicy{
    Mode:     backup.EncryptAES256,
    KeyFile:  &keyFilePath,
}
```

### Compression

ZSTD compression is supported with configurable compression levels:

```go
// For backup compression.
backupCfg.CompressionPolicy = &backup.CompressionPolicy{
    Mode:  backup.CompressZSTD,
    Level: 3,
}
```

```go
// For restore compressed backup.
restoreCfg.CompressionPolicy = &backup.CompressionPolicy{
    Mode:  backup.CompressZSTD,
    Level: 3,
}
```

### Partition Filters

Backup specific partitions or ranges:

```go
backupCfg.PartitionFilters = []*aerospike.PartitionFilter{
  // Filter by partition range.
  backup.NewPartitionFilterByRange(0, 100),
  // Filter by partition id.
  backup.NewPartitionFilterByID(200),
  // Filter by partition by exact partition digest.
  backup.NewPartitionFilterByDigest("source-ns1", "/+Ptyjj06wW9zx0AnxOmq45xJzs=")
  // Filter all records after digest.
  backup.NewPartitionFilterAfterDigest("source-ns1", "/+Ptyjj06wW9zx0AnxOmq45xJzs=")
}
```

## Prerequisites

- Go v1.23.0+
- [Aerospike Go client](https://github.com/aerospike/aerospike-client-go) v8
- [Mockery](https://github.com/vektra/mockery) for test mocks

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) file for details.
