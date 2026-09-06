# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This module is at `v0.x` and makes no API compatibility promise, so a minor bump may
change exported names. Read the entry for a release before upgrading.

Entries were reconstructed from the GitHub release notes of each tag. Follow the pull
request links for the full detail of any change.

## [Unreleased]

### Added

- Set indexes are backed up and restored. The ASB format is bumped to 3.3 to carry them, so a
  backup that contains a set index cannot be read by an older release.
  ([#501](https://github.com/aerospike/backup-go/pull/501), BKRS-334)
- Error classes. Every error a caller can act on now carries exactly one of `ErrInvalidConfig`,
  `ErrNotFound`, `ErrStorage`, `ErrCorruptData`, `ErrUnsupported`, `ErrAerospike` and
  `ErrSecretAgent`, so a failure can be handled with `errors.Is` instead of matching message text.
  The classes live in the new leaf package `github.com/aerospike/backup-go/errclass` and are
  re-exported from the root package as `backup.ErrInvalidConfig` and so on. The values are
  identical, so `errors.Is` matches either spelling. Which class an error carries is part of the
  library contract; the message text is not. (BKRS-364)
- Segment validation for server-integrated backups, in the new package `pkg/server/segvalidator`:
  it walks the segments of a backup in local or S3 storage and reports missing, truncated,
  mis-sized and checksum-mismatched ones.
  ([#505](https://github.com/aerospike/backup-go/pull/505), BKRS-344)
- `ClusterInfo.GetClusterStable`, which reports whether the cluster key is the same on every
  node, and a lister for server-integrated backups in `pkg/server/lister` that reads and sorts
  their metadata from S3. ([#470](https://github.com/aerospike/backup-go/pull/470), SERVER-898)
- Fuzzy restore for server-integrated backups: `RequestRestore` accepts `FuzzyRestore` and `Path`.
  ([#483](https://github.com/aerospike/backup-go/pull/483), BKRS-255)
- `SecretAgentConfig.MinTLSVersion`, for deployments that still need a TLS version below the new
  1.2 floor. ([#516](https://github.com/aerospike/backup-go/pull/516), BKRS-361)
- Repository documentation and process files: a security policy, a code of conduct, issue and
  pull request templates, a release script and this changelog.
  ([#518](https://github.com/aerospike/backup-go/pull/518), BKRS-363)

### Changed

- **Breaking.** The Secret Agent client moved from `pkg/secret-agent` to `pkg/secretagent`.
  ([#519](https://github.com/aerospike/backup-go/pull/519), BKRS-365)
- **Breaking.** Generic type parameters were removed from the encoding and pipeline types.
  `EncoderType`, `EncoderTypeASB` and `models.TokenConstraint` are gone, and `Encoder`, `Decoder`
  and the pipeline types are no longer generic. `ConfigRestore.EncoderType` no longer selects an
  encoder: ASB is the only format.
  ([#512](https://github.com/aerospike/backup-go/pull/512), BKRS-360)
- **Breaking.** `Client.Backup` returns the `BackupHandler` interface instead of `*BackupHandler`,
  and the restore handler interface was renamed from `Restorer` to `RestoreHandler`. Both make the
  handlers straightforward to fake in a caller's tests.
  ([#503](https://github.com/aerospike/backup-go/pull/503))
- **Breaking.** `InfoGetter` was split into `ClusterInfo` and `ServerBackupInfo`, which it now
  composes, so a caller can depend on the half it uses.
  ([#484](https://github.com/aerospike/backup-go/pull/484))
- **Breaking.** `asb.NewEncoderConfig` takes a `models.SIndexInfo` instead of a single
  `hasExpressionSIndex` flag. ([#501](https://github.com/aerospike/backup-go/pull/501), BKRS-334)
- Error messages are now prefixed with the class they belong to, for example
  `storage error: failed to open root /backups: permission denied`. Code that matches on message
  text should move to `errors.Is`. (BKRS-364)
- `Client.Backup` now rejects a nil writer, and a nil reader when it is asked to continue from a
  state file; `Client.Restore` now rejects a nil streaming reader. All three fail immediately with
  `ErrInvalidConfig` instead of failing later and less clearly. (BKRS-364)
- Server-integrated backup and restore commands are sent only to principal nodes.
  ([#485](https://github.com/aerospike/backup-go/pull/485), BKRS-258)
- Server-integrated backup state processing was reworked and its response models moved to
  `pkg/asinfo/models`; the metadata struct of the backup lister was trimmed to what the server
  actually reports. ([#490](https://github.com/aerospike/backup-go/pull/490),
  [#492](https://github.com/aerospike/backup-go/pull/492), BKRS-323, BKRS-325)
- The backup file limit is validated through a `NoChunkLimit` capability on the storage options
  rather than a check for the local backend, so the root package no longer imports a concrete
  storage implementation. Behaviour is unchanged except for stdout, which no longer reports a
  misleading chunk size error for a non-zero file limit.
  ([#513](https://github.com/aerospike/backup-go/pull/513), BKRS-359)
- The test suite is split into hermetic unit tests and Docker-backed integration tests behind the
  `integration` build tag, with CI, linting and `make test-unit` aligned with that split. Go is
  now 1.25.13 and the Aerospike client v8.8.0.
  ([#517](https://github.com/aerospike/backup-go/pull/517),
  [#482](https://github.com/aerospike/backup-go/pull/482), BKRS-362)
- CI workflows also run for the `dev` branch, and the pinned GitHub actions were updated.
  ([#486](https://github.com/aerospike/backup-go/pull/486),
  [#474](https://github.com/aerospike/backup-go/pull/474),
  [#478](https://github.com/aerospike/backup-go/pull/478),
  [#480](https://github.com/aerospike/backup-go/pull/480),
  [#481](https://github.com/aerospike/backup-go/pull/481))
- Package documentation and the README were rewritten, including the supported import paths.
  ([#515](https://github.com/aerospike/backup-go/pull/515),
  [#518](https://github.com/aerospike/backup-go/pull/518), BKRS-357, BKRS-363)

### Fixed

- A failed job that was cancelled at the same moment reported `context canceled` instead of the
  error that actually caused it, because both were ready and the wait picked one at random. The
  job error now wins. Failing to remove the state file no longer turns a finished backup into a
  failed one either. ([#514](https://github.com/aerospike/backup-go/pull/514), BKRS-358)
- Backup state is flushed and its files closed before the handler shuts down, so a continuation
  can no longer read a half-written state file and fail with `EOF`.
  ([#489](https://github.com/aerospike/backup-go/pull/489), BKRS-324)

### Removed

- **Breaking.** XDR backup support. `Client.BackupXDR`, `ConfigBackupXDR`, `HandlerBackupXDR`,
  the `XDRInfo` interface and the `io/aerospike/xdr` and `io/encoding/asbx` packages are gone,
  along with the ASBX format. ([#495](https://github.com/aerospike/backup-go/pull/495), BKRS-326)

### Security

- Local and cloud storage path handling was hardened: directory operations go through
  `os.Root`-scoped access, stat-then-act patterns were replaced to close TOCTOU windows,
  directories and files are created with `0700` and `0600`, and file names and object keys are
  validated against `..` segments, leading slashes and NUL bytes before anything touches storage.
  ([#488](https://github.com/aerospike/backup-go/pull/488), BKRS-271)
- Values of sensitive info command parameters (`access-key`, `secret-key`) are redacted from
  errors raised by the asinfo client, and the Secret Agent TLS configuration now defaults to a
  TLS 1.2 floor instead of accepting any version.
  ([#516](https://github.com/aerospike/backup-go/pull/516), BKRS-361)

## [0.11.1] - 2026-08-19

### Fixed

- A secondary index of a type the library does not recognise no longer fails the whole
  namespace backup. Such indexes are logged and skipped instead. This affects set indexes,
  introduced in Aerospike Database 8.1.2, which remain unsupported for backup.
  ([#502](https://github.com/aerospike/backup-go/pull/502), BKRS-334)

## [0.11.0] - 2026-07-15

### Added

- Scan throttling, to cap the load a backup puts on the cluster.
  ([#406](https://github.com/aerospike/backup-go/pull/406), BKRS-8)
- A retry policy for the secondary index and UDF creation commands issued during a restore,
  so a transient cluster error no longer aborts the restore.
  ([#414](https://github.com/aerospike/backup-go/pull/414), BKRS-145)
- Validation of the backup file limit, which now rejects invalid values instead of
  accepting them. ([#448](https://github.com/aerospike/backup-go/pull/448), BKRS-187)

### Changed

- Backup token encoding allocates less memory.
  ([#431](https://github.com/aerospike/backup-go/pull/431))
- Backups to S3 use less memory.
  ([#428](https://github.com/aerospike/backup-go/pull/428), BKRS-155)
- The restore decoder is faster.
  ([#442](https://github.com/aerospike/backup-go/pull/442), BKRS-182)
- Progress is reported with two decimal places and more often, so a long job no longer
  looks stalled. ([#466](https://github.com/aerospike/backup-go/pull/466), BKRS-218)
- Estimate printing moved into a shared component, and a restore now prints the file count
  before it starts. ([#452](https://github.com/aerospike/backup-go/pull/452),
  [#454](https://github.com/aerospike/backup-go/pull/454), BKRS-190)
- Clearer error text for encryption configuration problems.
  ([#440](https://github.com/aerospike/backup-go/pull/440), BKRS-129)
- Internal cleanup of the storage readers, with no change in behaviour.
  ([#437](https://github.com/aerospike/backup-go/pull/437), BKRS-178)

### Fixed

- A cancelled backup released its limiter slots, so later backups could not start.
  ([#409](https://github.com/aerospike/backup-go/pull/409), BKRS-114)
- Retries of large resumable uploads to Google Cloud Storage were effectively disabled,
  because the backoff outlasted the per-chunk retry deadline. Transient 429 and 503
  responses during highly parallel backups are now retried.
  ([#476](https://github.com/aerospike/backup-go/pull/476), BKRS-244)
- Record count errors are logged at WARN rather than ERROR, as they are not fatal.
  ([#429](https://github.com/aerospike/backup-go/pull/429), BKRS-156)
- The internal client ID no longer appears in log messages.
  ([#418](https://github.com/aerospike/backup-go/pull/418), BKRS-148)

## [0.10.1] - 2026-08-27

Backport of the 0.11.1 fix onto the 0.10 line. Released after 0.11.1.

### Fixed

- Unrecognised secondary index types are logged and skipped instead of failing the backup.
  ([#502](https://github.com/aerospike/backup-go/pull/502), BKRS-334)

## [0.10.0] - 2026-03-10

### Changed

- Faster escaping in the encoder.
  ([#374](https://github.com/aerospike/backup-go/pull/374), FMWK-893)
- Redundant context passing removed.
  ([#375](https://github.com/aerospike/backup-go/pull/375), FMWK-894)
- Faster integer parsing in the decoder.
  ([#379](https://github.com/aerospike/backup-go/pull/379), FMWK-881)
- Debug logs are quieter. ([#400](https://github.com/aerospike/backup-go/pull/400), BKRS-87)
- The scan limiter reports more about its state.
  ([#403](https://github.com/aerospike/backup-go/pull/403))
- Error and log message wording normalised across the library.
  ([#405](https://github.com/aerospike/backup-go/pull/405))
- The save command checks the context.
  ([#386](https://github.com/aerospike/backup-go/pull/386), FMWK-915)

### Fixed

- Size calculation for nested directories.
  ([#383](https://github.com/aerospike/backup-go/pull/383), FMWK-909)
- Aerospike client updated to pick up a drop index fix.
  ([#408](https://github.com/aerospike/backup-go/pull/408), BKRS-26)

## [0.9.0] - 2026-01-12

### Added

- A counter of retry attempts for write operations.
  ([#356](https://github.com/aerospike/backup-go/pull/356), FMWK-849)
- A configurable buffer size for the local reader.
  ([#359](https://github.com/aerospike/backup-go/pull/359), FMWK-860)
- Metadata is carried inside a single-file backup.
  ([#362](https://github.com/aerospike/backup-go/pull/362), APPS-1971)
- The node list is retried inside the version info command.
  ([#363](https://github.com/aerospike/backup-go/pull/363), FMWK-864)
- Full TLS support in the secret agent configuration.
  ([#368](https://github.com/aerospike/backup-go/pull/368), APPS-1991)
- Base64-encoded keys are accepted.
  ([#369](https://github.com/aerospike/backup-go/pull/369), APPS-2007)
- Single-line PEM files are accepted.
  ([#370](https://github.com/aerospike/backup-go/pull/370), APPS-2008)

### Changed

- Aerospike client updated to v8.4.2.
  ([#360](https://github.com/aerospike/backup-go/pull/360))
- S3 operations optimised.
  ([#344](https://github.com/aerospike/backup-go/pull/344), FMWK-816)
- Azure Blob Storage IO optimised.
  ([#353](https://github.com/aerospike/backup-go/pull/353), FMWK-836)
- Writer initialisation refactored.
  ([#358](https://github.com/aerospike/backup-go/pull/358), FMWK-856)

### Fixed

- The records-per-second limiter.
  ([#354](https://github.com/aerospike/backup-go/pull/354), FMWK-851)
- The in-doubt counter for batch writes.
  ([#355](https://github.com/aerospike/backup-go/pull/355), FMWK-850)
- A goroutine leaked when a backup completed successfully.
  ([#371](https://github.com/aerospike/backup-go/pull/371), FMWK-882)
- Checksum validation on Google Cloud Storage.
  ([#372](https://github.com/aerospike/backup-go/pull/372), APPS-2010)
- Estimate calculation for a set that does not exist.
  ([#373](https://github.com/aerospike/backup-go/pull/373), APPS-2011)

## [0.8.0] - 2025-11-05

### Added

- Metadata is written to dedicated backup files.
  ([#338](https://github.com/aerospike/backup-go/pull/338), FMWK-799)
- Expression-based secondary indexes are backed up.
  ([#335](https://github.com/aerospike/backup-go/pull/335), FMWK-797)
- Retryable readers for Azure Blob Storage and Google Cloud Storage.
  ([#340](https://github.com/aerospike/backup-go/pull/340),
  [#341](https://github.com/aerospike/backup-go/pull/341), FMWK-818, FMWK-819)
- Total path size calculation can be turned off.
  ([#347](https://github.com/aerospike/backup-go/pull/347), FMWK-843)
- Validation rejecting duplicated elements in configs.
  ([#349](https://github.com/aerospike/backup-go/pull/349), FMWK-847)

### Changed

- The retry policy was reworked.
  ([#342](https://github.com/aerospike/backup-go/pull/342), FMWK-817)
- Availability-zone aware backups optimised.
  ([#334](https://github.com/aerospike/backup-go/pull/334), FMWK-813)
- Better error handling and messages, including errors from the ZSTD library and the case
  of missing rack nodes. ([#343](https://github.com/aerospike/backup-go/pull/343),
  [#346](https://github.com/aerospike/backup-go/pull/346),
  [#351](https://github.com/aerospike/backup-go/pull/351),
  [#352](https://github.com/aerospike/backup-go/pull/352))

### Fixed

- Record calculation for a rack list.
  ([#348](https://github.com/aerospike/backup-go/pull/348), FMWK-846)
- Backup size estimation refined.
  ([#339](https://github.com/aerospike/backup-go/pull/339))

## [0.7.0] - 2025-09-17

### Added

- Backup and restore through standard input and output.
  ([#332](https://github.com/aerospike/backup-go/pull/332), APPS-1891)
- A retryable S3 reader.
  ([#336](https://github.com/aerospike/backup-go/pull/336), APPS-1912)

### Fixed

- The skip counter was not incremented when restoring with the no-records flag.
  ([#333](https://github.com/aerospike/backup-go/pull/333), FMWK-811)

## [0.6.0] - 2025-08-20

### Removed

- **Breaking.** The backup CLI tools no longer ship from this repository. They live in
  [aerospike-backup-cli](https://github.com/aerospike/aerospike-backup-cli).
  ([#320](https://github.com/aerospike/backup-go/pull/320), APPS-1804)

### Added

- Versioning of asinfo commands.
  ([#324](https://github.com/aerospike/backup-go/pull/324), FMWK-795)
- An exported getter for the info client.
  ([#327](https://github.com/aerospike/backup-go/pull/327), FMWK-805)

### Changed

- The reader was reworked for sequential processing.
  ([#331](https://github.com/aerospike/backup-go/pull/331), FMWK-807)
- Expression secondary indexes are ignored.
  ([#321](https://github.com/aerospike/backup-go/pull/321), FMWK-798)
- The bandwidth limiter and the scan limiting mechanism were improved.
  ([#315](https://github.com/aerospike/backup-go/pull/315),
  [#330](https://github.com/aerospike/backup-go/pull/330), FMWK-788, FMWK-806)
- The file writer closing mechanism was reworked.
  ([#322](https://github.com/aerospike/backup-go/pull/322), APPS-1820)
- Stricter partition filter validation, and clearer text for invalid token and info command
  errors. ([#317](https://github.com/aerospike/backup-go/pull/317),
  [#323](https://github.com/aerospike/backup-go/pull/323),
  [#326](https://github.com/aerospike/backup-go/pull/326))

### Fixed

- A race when metrics were read from a closed pipeline.
  ([#325](https://github.com/aerospike/backup-go/pull/325), APPS-1849)

### Security

- GitHub workflows run with minimal permissions.
  ([#318](https://github.com/aerospike/backup-go/pull/318))

## [0.5.1] - 2025-07-08

### Changed

- The context error check was removed from IO operations.
  ([#313](https://github.com/aerospike/backup-go/pull/313), APPS-1798)

## [0.5.0] - 2025-07-07

### Added

- A retry policy for the cloud storage providers.
  ([#290](https://github.com/aerospike/backup-go/pull/290), FMWK-745)
- Backup file validation.
  ([#296](https://github.com/aerospike/backup-go/pull/296), FMWK-763)
- Docker images for the CLI tools.
  ([#297](https://github.com/aerospike/backup-go/pull/297), FMWK-773)
- Validation of the backup continuation flags.
  ([#298](https://github.com/aerospike/backup-go/pull/298), FMWK-774)
- Validation of prefer-racks.
  ([#306](https://github.com/aerospike/backup-go/pull/306), FMWK-787)

### Changed

- The processing pipeline architecture was simplified.
  ([#292](https://github.com/aerospike/backup-go/pull/292), FMWK-737)
- The ASB decoder is faster.
  ([#293](https://github.com/aerospike/backup-go/pull/293), FMWK-758)
- Errors from a batch write retry are joined and de-duplicated.
  ([#289](https://github.com/aerospike/backup-go/pull/289))
- Mockery updated to v3.
  ([#288](https://github.com/aerospike/backup-go/pull/288), FMWK-752)

### Fixed

- A memory leak during restore.
  ([#309](https://github.com/aerospike/backup-go/pull/309), APPS-1794)
- A race in the pipeline metrics.
  ([#301](https://github.com/aerospike/backup-go/pull/301), FMWK-778)
- The reader chain did not stop when the context was cancelled.
  ([#304](https://github.com/aerospike/backup-go/pull/304), APPS-1765)
- A limiter burst error, and bandwidth limiter overhead.
  ([#305](https://github.com/aerospike/backup-go/pull/305),
  [#308](https://github.com/aerospike/backup-go/pull/308), APPS-1779, APPS-1788)
- Rack-list backup against Aerospike Database 6.
  ([#291](https://github.com/aerospike/backup-go/pull/291), FMWK-759)
- The state file is removed once a backup finishes.
  ([#294](https://github.com/aerospike/backup-go/pull/294), FMWK-702)
- Empty files are skipped on restore.
  ([#295](https://github.com/aerospike/backup-go/pull/295), FMWK-766)
- Estimate calculation when restoring from a directory list.
  ([#299](https://github.com/aerospike/backup-go/pull/299), APPS-1748)
- Integer type conversion in the decoder.
  ([#310](https://github.com/aerospike/backup-go/pull/310))

### Removed

- Total Records was dropped from the report, and context processing improved.
  ([#300](https://github.com/aerospike/backup-go/pull/300), APPS-1716)

## [0.4.0] - 2025-05-15

### Added

- Access key authentication for AWS.
  ([#199](https://github.com/aerospike/backup-go/pull/199), FMWK-642)
- Rack-aware backups.
  ([#263](https://github.com/aerospike/backup-go/pull/263), FMWK-722)
- Restore from archived files.
  ([#240](https://github.com/aerospike/backup-go/pull/240), FMWK-683)
- Object storage class configuration.
  ([#234](https://github.com/aerospike/backup-go/pull/234), APPS-1503)
- A configurable chunk size for the writers.
  ([#273](https://github.com/aerospike/backup-go/pull/273), FMWK-742)
- An in-doubt counter.
  ([#258](https://github.com/aerospike/backup-go/pull/258), APPS-1552)
- Records-per-second and kilobytes-per-second metrics for the reader and the writer.
  ([#265](https://github.com/aerospike/backup-go/pull/265),
  [#267](https://github.com/aerospike/backup-go/pull/267), FMWK-728, FMWK-730)
- MAX_RETRIES_EXCEEDED counts as retriable.
  ([#285](https://github.com/aerospike/backup-go/pull/285), APPS-1574)

### Changed

- Aerospike client updated to v8.
  ([#228](https://github.com/aerospike/backup-go/pull/228))
- Base64 encoding is faster, using segmentio/asm/base64.
  ([#266](https://github.com/aerospike/backup-go/pull/266),
  [#268](https://github.com/aerospike/backup-go/pull/268), FMWK-731, FMWK-735)
- Encoding formatting functions optimised, and record number estimation made cheaper.
  ([#271](https://github.com/aerospike/backup-go/pull/271),
  [#275](https://github.com/aerospike/backup-go/pull/275), FMWK-741, FMWK-732)
- The storage IO package structure was reorganised.
  ([#229](https://github.com/aerospike/backup-go/pull/229), FMWK-677)
- Node list filtering reworked and the command structure reorganised.
  ([#279](https://github.com/aerospike/backup-go/pull/279), FMWK-727)
- Linting moved to golangci-lint v2.
  ([#260](https://github.com/aerospike/backup-go/pull/260), FMWK-707)

### Fixed

- The total records counter in the metrics.
  ([#274](https://github.com/aerospike/backup-go/pull/274), FMWK-741)
- The records-per-second limiter in parallel mode.
  ([#280](https://github.com/aerospike/backup-go/pull/280), FMWK-743)
- Parallelism when filtering by node list.
  ([#283](https://github.com/aerospike/backup-go/pull/283), APPS-1688)
- Reported file size for compressed and encrypted files.
  ([#244](https://github.com/aerospike/backup-go/pull/244), FMWK-690)
- PKCS1 key parsing.
  ([#245](https://github.com/aerospike/backup-go/pull/245), FMWK-692)
- The record count at namespace level.
  ([#247](https://github.com/aerospike/backup-go/pull/247), APPS-1547)

## [0.3.1] - 2024-12-24

### Fixed

- ASB encoding of float values.
  ([#189](https://github.com/aerospike/backup-go/pull/189), APPS-1385)

## [0.3.0] - 2024-12-02

### Added

- Restore accepts a list of directories.
  ([#188](https://github.com/aerospike/backup-go/pull/188), FMWK-620)

### Changed

- Files and folders are created lazily by the common writer.
  ([#184](https://github.com/aerospike/backup-go/pull/184),
  [#186](https://github.com/aerospike/backup-go/pull/186), FMWK-612, FMWK-619)

### Fixed

- User keys are restored.
  ([#185](https://github.com/aerospike/backup-go/pull/185), FMWK-613)
- Backup of restored records that carry no user key.
  ([#187](https://github.com/aerospike/backup-go/pull/187))

## [0.2.0] - 2024-11-21

First beta of the `asbackup` and `asrestore` tools written in Go, released from this
repository alongside the library.

### Added

- Google Cloud Storage and Azure Blob Storage backends.
  ([#120](https://github.com/aerospike/backup-go/pull/120),
  [#135](https://github.com/aerospike/backup-go/pull/135), FMWK-538, FMWK-542)
- Single-file backup.
  ([#123](https://github.com/aerospike/backup-go/pull/123), FMWK-393)
- Backup state and continuation, so an interrupted job can be resumed.
  ([#158](https://github.com/aerospike/backup-go/pull/158), FMWK-570)
- Per-node backup parallelism, and node-list, partition-list and after-digest filtering.
  ([#145](https://github.com/aerospike/backup-go/pull/145),
  [#149](https://github.com/aerospike/backup-go/pull/149),
  [#153](https://github.com/aerospike/backup-go/pull/153),
  [#130](https://github.com/aerospike/backup-go/pull/130))
- The estimate operation.
  ([#154](https://github.com/aerospike/backup-go/pull/154), FMWK-568)
- Options for extra TTL, ignoring permanent errors, removing artifacts, compact, no-TTL-only
  and prefer-racks. ([#127](https://github.com/aerospike/backup-go/pull/127),
  [#128](https://github.com/aerospike/backup-go/pull/128),
  [#146](https://github.com/aerospike/backup-go/pull/146),
  [#148](https://github.com/aerospike/backup-go/pull/148),
  [#151](https://github.com/aerospike/backup-go/pull/151),
  [#152](https://github.com/aerospike/backup-go/pull/152))
- A read-after parameter for the cloud readers.
  ([#155](https://github.com/aerospike/backup-go/pull/155), FMWK-559)
- Secret agent support for the cloud configuration.
  ([#161](https://github.com/aerospike/backup-go/pull/161), FMWK-519)
- Client and batch policy timeouts are exposed.
  ([#177](https://github.com/aerospike/backup-go/pull/177),
  [#181](https://github.com/aerospike/backup-go/pull/181), FMWK-610, FMWK-611)

### Changed

- IO operations were pulled out of the configuration structs.
  ([#121](https://github.com/aerospike/backup-go/pull/121), FMWK-529)
- The restore process was refactored, and the handler's context revised.
  ([#122](https://github.com/aerospike/backup-go/pull/122),
  [#137](https://github.com/aerospike/backup-go/pull/137), FMWK-531)
- Go 1.22 is required.
  ([#178](https://github.com/aerospike/backup-go/pull/178), FMWK-592)

### Fixed

- Restore failed when records were skipped.
  ([#168](https://github.com/aerospike/backup-go/pull/168), APPS-1327)
- Records with no bins are skipped on restore.
  ([#159](https://github.com/aerospike/backup-go/pull/159), APPS-1309)
- The modified-before, modified-after and after-digest filters.
  ([#160](https://github.com/aerospike/backup-go/pull/160),
  [#163](https://github.com/aerospike/backup-go/pull/163), APPS-1314, APPS-1312)
- The filter expression in the record reader.
  ([#180](https://github.com/aerospike/backup-go/pull/180), APPS-1346)
- A nil check before releasing the semaphore in the record reader.
  ([#129](https://github.com/aerospike/backup-go/pull/129))
- S3 directory checking, backup listing markers, and errors while streaming files from a
  directory. ([#166](https://github.com/aerospike/backup-go/pull/166),
  [#170](https://github.com/aerospike/backup-go/pull/170),
  [#174](https://github.com/aerospike/backup-go/pull/174),
  [#176](https://github.com/aerospike/backup-go/pull/176))
- The Google Cloud Storage reader stops iterating on any error.
  ([#138](https://github.com/aerospike/backup-go/pull/138))
- Missing restore configuration mappings.
  ([#157](https://github.com/aerospike/backup-go/pull/157), APPS-1304)
- Parsing of an info command result containing a secret.
  ([#164](https://github.com/aerospike/backup-go/pull/164), FMWK-584)

## [0.1.0] - 2024-08-21

Initial release.

[Unreleased]: https://github.com/aerospike/backup-go/compare/v0.11.1...HEAD
[0.11.1]: https://github.com/aerospike/backup-go/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/aerospike/backup-go/compare/v0.10.0...v0.11.0
[0.10.1]: https://github.com/aerospike/backup-go/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/aerospike/backup-go/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/aerospike/backup-go/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/aerospike/backup-go/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/aerospike/backup-go/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/aerospike/backup-go/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/aerospike/backup-go/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/aerospike/backup-go/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/aerospike/backup-go/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/aerospike/backup-go/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/aerospike/backup-go/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/aerospike/backup-go/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/aerospike/backup-go/releases/tag/v0.1.0
