# Aerospike backup (asbackup)
Aerospike Backup CLI tool. This page describes capabilities and configuration options of the Aerospike backup tool, `asbackup`.

## Overview
`asbackup` backs up data from an Aerospike database according to a user-defined scope of specific namespaces, sets, or both. The scope supports further refinement with partition or time-based filters.

After you define the scope, `asbackup` scans the database and fetches the records that match the specified criteria. `asbackup` captures only the essential data needed for recovery and ignores non-critical system or secondary data.

As `asbackup` identifies records for backup, it serializes the data into a predefined format and writes it to a backup file or directory. Serialization converts the in-memory representation of records into a stable format that can be safely stored on disk.

`asbackup` supports backing up locally, to an Amazon S3 bucket, to an Azure container or GCP bucket.

## `asbackup` limitations
`asbackup` has the following limitations:

- `asbackup` requires read privileges or higher. See [Configuring Access Control in EE and FE](https://aerospike.com/docs/database/manage/security/rbac/#privileges) for more information.
- Direct backups are supported to S3, Azure Blob, GCP Storage, also you can use other services for storing the backup files after creating them locally.
- When running in directory mode, each parallel worker creates its own backup file.
- You can control the size of backup files created by `asbackup` with the `--file-limit` option. After a backup file reaches the predefined size, `asbackup` creates another file. `asbackup` does not have an upper limit for the size of a backup file.
- zstd is the only compression algorithm available with `asbackup`.

## Default backup content
`asbackup` backs up the following data by default:

- Keys
  - Key metadata: digest, TTL, generation count, and key
  - Regular bins: string, integer, boolean, and binary
  - Collection data type (CDT) bins: list and map
  - GeoJSON data type bins
  - HyperLogLog data type bins
- Secondary index definitions
- User-Defined Function (UDF) modules

---

## Build
### Dev
```bash
make build
```
### Release
Version artifacts are automatically built and uploaded under releases in GitHub.

## Supported flags
```bash
Usage:
  asbackup [flags]

General Flags:
  -Z, --help               Display help information.
  -V, --version            Display version information.
  -v, --verbose            Enable more detailed logging.
      --log-level string   Determine log level for --verbose output. Log levels are: debug, info, warn, error. (default "debug")
      --log-json           Set output in JSON format for parsing by external tools.
      --config string      Path to YAML configuration file.

Aerospike Client Flags:
  -h, --host host[:tls-name][:port][,...]                                                           The Aerospike host. (default 127.0.0.1)
  -p, --port int                                                                                    The default Aerospike port. (default 3000)
  -U, --user string                                                                                 The Aerospike user to use to connect to the Aerospike cluster.
  -P, --password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"                   The Aerospike password to use to connect to the Aerospike 
                                                                                                    cluster.
      --auth INTERNAL,EXTERNAL,PKI                                                                  The authentication mode used by the Aerospike server. INTERNAL 
                                                                                                    uses standard user/pass. EXTERNAL uses external methods (like LDAP) 
                                                                                                    which are configured on the server. EXTERNAL requires TLS. PKI allows 
                                                                                                    TLS authentication and authorization based on a certificate. No 
                                                                                                    username needs to be configured. (default INTERNAL)
      --tls-enable                                                                                  Enable TLS authentication with Aerospike. If false, other TLS 
                                                                                                    options are ignored.
      --tls-name string                                                                             The server TLS context to use to authenticate the connection to 
                                                                                                    Aerospike.
      --tls-cafile env-b64:<cert>,b64:<cert>,<cert-file-name>                                       The CA used when connecting to Aerospike.
      --tls-capath <cert-path-name>                                                                 A path containing CAs for connecting to Aerospike.
      --tls-certfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                     The certificate file for mutual TLS authentication with 
                                                                                                    Aerospike.
      --tls-keyfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                      The key file used for mutual TLS authentication with Aerospike.
      --tls-keyfile-password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"       The password used to decrypt the key file if encrypted.
      --tls-protocols "[[+][-]all] [[+][-]TLSv1] [[+][-]TLSv1.1] [[+][-]TLSv1.2] [[+][-]TLSv1.3]"   Set the TLS protocol selection criteria. This format is the same 
                                                                                                    as Apache's SSLProtocol documented at 
                                                                                                    https://httpd.apache.org/docs/current/mod/mod_ssl.html#ssl protocol. (default +TLSv1.2)
      --client-timeout int         Initial host connection timeout duration. The timeout when opening a connection
                                   to the server host for the first time. (default 30000)
      --client-idle-timeout int    Idle timeout. Every time a connection is used, its idle
                                   deadline will be extended by this duration. When this deadline is reached,
                                   the connection will be closed and discarded from the connection pool.
                                   The value is limited to 24 hours (86400s).
                                   It's important to set this value to a few seconds less than the server's proto-fd-idle-ms
                                   (default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket
                                   that has already been reaped by the server.
                                   Connection pools are now implemented by a LIFO stack. Connections at the tail of the
                                   stack will always be the least used. These connections are checked for IdleTimeout
                                   on every tend (usually 1 second).
                                   
      --client-login-timeout int   Specifies the login operation timeout for external authentication methods such as LDAP. (default 10000)

Backup Flags:
  -d, --directory string         The directory that holds the backup files. Required, unless -o or -e is used.
  -n, --namespace string         The namespace to be backed up. Required.
  -s, --set string               The set(s) to be backed up. Accepts comma-separated values with no spaces: 'set1,set2,set3'
                                 If multiple sets are being backed up, filter-exp cannot be used.
                                 If empty, include all sets.
  -B, --bin-list string          Only include the given bins in the backup.
                                 Accepts comma-separated values with no spaces: 'bin1,bin2,bin3'
                                 If empty include all bins.
  -R, --no-records               Don't back up any records.
  -I, --no-indexes               Don't back up any indexes.
      --no-udfs                  Don't back up any UDFs.
  -w, --parallel int             Maximum number of scan calls to run in parallel.
                                 If only one partition range is given, or the entire namespace is being backed up, the range
                                 of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each
                                 filter cannot be parallelized individually, so you may only achieve as much parallelism as there are
                                 partition filters. Accepts values from 1-1024 inclusive. (default 1)
  -L, --records-per-second int   Limit total returned records per second (rps).
                                 Do not apply rps limit if records-per-second is zero.
      --max-retries int          Maximum number of retries before aborting the current transaction. (default 5)
      --total-timeout int        Total transaction timeout in milliseconds. 0 - no timeout.
      --socket-timeout int       Socket timeout in milliseconds. If this value is 0, it's set to --total-timeout.
                                 If both this and --total-timeout are 0, there is no socket idle time limit. (default 10000)
      --nice int                 The limits for read/write storage bandwidth in MiB/s.
                                 Default is 0 (no limit). (DEPRECATED: use --bandwidth instead)
  -N, --bandwidth int            The limits for read/write storage bandwidth in MiB/s.
                                 Default is 0 (no limit).
  -r, --remove-files                  Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.
      --remove-artifacts              Remove existing backup file (-o) or files (-d) without performing a backup.
  -o, --output-file string            Backup to a single backup file. Use - for stdout. Required, unless -d or -e is used.
  -q, --output-file-prefix string     When using directory parameter, prepend a prefix to the names of the generated files.
  -F, --file-limit uint               Rotate backup files when their size crosses the given
                                      value (in bytes). Only used when backing up to a directory.
                                       (default 262144000)
  -x, --no-bins                       Do not include bin data in the backup. Use this flag for data sampling or troubleshooting.
                                      On restore all records, that don't contain bin data will be skipped.
      --no-ttl-only                   Only include records that have no ttl set (persistent records).
  -D, --after-digest string           Backup records after record digest in record's partition plus all succeeding
                                      partitions. Used to resume backup with last record received from previous
                                      incomplete backup.
                                      This argument is mutually exclusive with partition-list.
                                      Format: Base64 encoded string
                                      Example: EjRWeJq83vEjRRI0VniavN7xI0U=
                                      
  -a, --modified-after string         <YYYY-MM-DD_HH:MM:SS>
                                      Perform an incremental backup; only include records 
                                      that changed after the given date and time. The system's 
                                      local timezone applies. If only HH:MM:SS is specified, then
                                      today's date is assumed as the date. If only YYYY-MM-DD is 
                                      specified, then 00:00:00 (midnight) is assumed as the time.
                                      
  -b, --modified-before string        <YYYY-MM-DD_HH:MM:SS>
                                      Only include records that last changed before the given
                                      date and time. May combined with --modified-after to specify a range.
  -f, --filter-exp string             Base64 encoded expression. Use the encoded filter expression in each scan call,
                                      which can be used to do a partial backup. The expression to be used can be Base64 
                                      encoded through any client. This argument is mutually exclusive with multi-set backup.
                                      
      --parallel-nodes                Specifies how to perform the query of the database run for each backup.
                                      By default, asbackup runs parallel workers for partitions.
                                      If this flag is set to true, asbackup launches parallel workers for nodes.
                                      The number of parallel workers is set by the --parallel flag.
                                      This option is mutually exclusive with --continue and --estimate.
  -l, --node-list string              <addr 1>:<port 1>[,<addr 2>:<port 2>[,...]]
                                      <node name 1>[,<node name 2>[,...]]
                                      To get the correct node address, use 'service-tls-std' if a database configured to use TLS
                                      and 'service-clear-std' info command if no TLS is configured.
                                      To get the node name, use the 'node:' info command.
                                      Back up the given cluster nodes only.
                                      The job is parallelized by number of nodes unless --parallel is set less than nodes number.
                                      This argument is mutually exclusive with --partition-list, --after-digest, --rack-list, --prefer-racks arguments.
                                      Default: backup all nodes in the cluster
  -X, --partition-list string         List of partitions <filter[,<filter>[...]]> to back up. Partition filters can be ranges,
                                      individual partitions, or records after a specific digest within a single partition.
                                      This argument is mutually exclusive with after-digest.
                                      Filter: <begin partition>[-<partition count>]|<digest>
                                      begin partition: 0-4095
                                      partition count: 1-4096 Default: 1
                                      digest: Base64 encoded string
                                      Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=
                                      Default: 0-4096 (all partitions)
                                      
      --prefer-racks string           <rack id 1>[,<rack id 2>[,...]]
                                      A list of Aerospike Database rack IDs to prefer when reading records for a backup.
                                      This argument is mutually exclusive with --rack-list and --node-list.
      --rack-list string              <rack id 1>[,<rack id 2>[,...]]
                                      A list of Aerospike Database rack IDs to backup.
                                      Unlike --prefer-racks, only specified racks will be backed up.
                                      This argument is mutually exclusive with --prefer-racks and --node-list.
  -M, --max-records int               The number of records approximately to back up. 0 - all records
      --sleep-between-retries int     The amount of milliseconds to sleep between retries after an error.
                                      This field is ignored when --max-retries is zero. (default 5)
  -C, --compact                       If true, do not apply base-64 encoding to BLOBs and instead write raw binary data,
                                      resulting in smaller backup files.
                                      Deprecated.
  -e, --estimate                      Estimate the backed-up record size from a random sample of 
                                      10,000 (default) records at 99.9999% confidence to estimate the full backup size.
                                      It ignores any filter:  --filter-exp, --node-list, --modified-after, --modified-before, --no-ttl-only,
                                      --after-digest, --partition-list.
      --estimate-samples int          The number of samples to take when running a backup estimate. (default 10000)
      --state-file-dst string         Name of a state file that will be saved in backup --directory.
                                      Works only with --file-limit parameter. As --file-limit is reached and the file is closed,
                                      the current state will be saved. Works only for default and/or partition backup.
                                      Not work with --parallel-nodes or --node--list.
  -c, --continue string               Resumes an interrupted/failed backup from where it was left off, given the .state file
                                      that was generated from the interrupted/failed run.
                                      --continue and --state-file-dst are mutually exclusive.
      --scan-page-size int            Number of records will be read on one iteration for continuation backup.
                                      Affects size if overlap on resuming backup after an error.
                                      Used only with --state-file-dst or --continue. (default 10000)
      --info-retry-timeout int        Set the initial timeout for a retry in milliseconds when info commands are sent. (default 1000)
      --info-retry-multiplier float   Increases the delay between subsequent retry attempts.
                                      The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber) (default 1)
      --info-max-retries uint         How many times to retry to send info commands before failing.  (default 3)

Compression Flags:
  -z, --compress string         Enables compressing of backup files using the specified compression algorithm.
                                Supported compression algorithms are: zstd, none
                                Set the zstd compression level via the --compression-level option. (default "NONE")
      --compression-level int   zstd compression level. (default 3)

Encryption Flags:
      --encrypt string                 Enables encryption of backup files using the specified encryption algorithm.
                                       Supported encryption algorithms are: none, aes128, aes256.
                                       A private key must be given, either via the --encryption-key-file option or
                                       the --encryption-key-env option or the --encryption-key-secret.
      --encryption-key-file string     Grabs the encryption key from the given file, which must be in PEM format.
      --encryption-key-env string      Grabs the encryption key from the given environment variable, which must be base-64 encoded.
      --encryption-key-secret string   Grabs the encryption key from secret-agent.

Secret Agent Flags:
Options pertaining to the Aerospike Secret Agent.
See documentation here: https://aerospike.com/docs/tools/secret-agent.
Both asbackup and asrestore support getting all the cloud config parameters from the Aerospike Secret Agent.
To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>' 
Example: asbackup --azure-account-name secret:resource1:azaccount
      --sa-connection-type string   Secret Agent connection type, supported types: tcp, unix. (default "tcp")
      --sa-address string           Secret Agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret Agent port (only for TCP connection).
      --sa-timeout int              Secret Agent connection and reading timeout.
      --sa-cafile string            Path to ca file for encrypted connections.
      --sa-is-base64                Whether Secret Agent responses are Base64 encoded.

AWS Flags:
For S3 storage bucket name is mandatory, and is set with --s3-bucket-name flag.
So --directory path will only contain folder name.
--s3-endpoint-override is used in case you want to use minio, instead of AWS.
Any AWS parameter can be retrieved from Secret Agent.
      --s3-bucket-name string         Existing S3 bucket name
      --s3-region string              The S3 region that the bucket(s) exist in.
      --s3-profile string             The S3 profile to use for credentials.
      --s3-access-key-id string       S3 access key id. If not set, profile auth info will be used.
      --s3-secret-access-key string   S3 secret access key. If not set, profile auth info will be used.
      --s3-endpoint-override string   An alternate url endpoint to send S3 API calls to.
      --s3-storage-class string       Apply storage class to backup files. Storage classes are:
                                      STANDARD,
                                      REDUCED_REDUNDANCY,
                                      STANDARD_IA,
                                      ONEZONE_IA,
                                      INTELLIGENT_TIERING,
                                      GLACIER,
                                      DEEP_ARCHIVE,
                                      OUTPOSTS,
                                      GLACIER_IR,
                                      SNOW,
                                      EXPRESS_ONEZONE.
      --s3-chunk-size int             Chunk size controls the maximum number of bytes of the object that the app will attempt to send to
                                      the storage in a single request. Objects smaller than the size will be sent in a single request,
                                      while larger objects will be split over multiple requests. (default 5242880)
      --s3-retry-max-attempts int     Maximum number of attempts that should be made in case of an error. (default 100)
      --s3-retry-max-backoff int      Max backoff duration in seconds between retried attempts. (default 90)
      --s3-retry-backoff int          Provides the backoff in seconds strategy the retryer will use to determine the delay between retry attempts. (default 60)

GCP Flags:
For GCP storage bucket name is mandatory, and is set with --gcp-bucket-name flag.
So --directory path will only contain folder name.
Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.
Any GCP parameter can be retrieved from Secret Agent.
      --gcp-key-path string                  Path to file containing service account JSON key.
      --gcp-bucket-name string               Name of the Google cloud storage bucket.
      --gcp-endpoint-override string         An alternate url endpoint to send GCP API calls to.
      --gcp-chunk-size int                   Chunk size controls the maximum number of bytes of the object that the app will attempt to send to
                                             the storage in a single request. Objects smaller than the size will be sent in a single request,
                                             while larger objects will be split over multiple requests. (default 5242880)
      --gcp-retry-max-attempts int           Max retries specifies the maximum number of attempts a failed operation will be retried
                                             before producing an error. (default 100)
      --gcp-retry-max-backoff int            Max backoff is the maximum value in seconds of the retry period. (default 90)
      --gcp-retry-init-backoff int           Initial backoff is the initial value in seconds of the retry period. (default 60)
      --gcp-retry-backoff-multiplier float   Multiplier is the factor by which the retry period increases.
                                             It should be greater than 1. (default 2)

Azure Flags:
For Azure storage container name is mandatory, and is set with --azure-storage-container-name flag.
So --directory path will only contain folder name.
Flag --azure-endpoint is optional, and is used for tests with Azurit or any other Azure emulator.
For authentication you can use --azure-account-name and --azure-account-key, or 
--azure-tenant-id, --azure-client-id and azure-client-secret.
Any Azure parameter can be retrieved from Secret Agent.
      --azure-account-name string      Azure account name for account name, key authorization.
      --azure-account-key string       Azure account key for account name, key authorization.
      --azure-tenant-id string         Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string         Azure client ID for Azure Active Directory authorization.
      --azure-client-secret string     Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string          Azure endpoint.
      --azure-container-name string    Azure container Name.
      --azure-access-tier string       Azure access tier is applied to created backup files.
                                       Tiers are: Archive, Cold, Cool, Hot, P10, P15, P20, P30, P4, P40, P50, P6, P60, P70, P80, Premium.
      --azure-block-size int           Block size defines the size of the buffer used during upload. (default 5242880)
      --azure-retry-max-attempts int   Max retries specifies the maximum number of attempts a failed operation will be retried
                                       before producing an error. (default 100)
      --azure-retry-max-delay int      Max retry delay specifies the maximum delay in seconds allowed before retrying an operation.
                                       Typically the value is greater than or equal to the value specified in azure-retry-delay. (default 90)
      --azure-retry-delay int          Retry delay specifies the initial amount of delay in seconds to use before retrying an operation.
                                       The value is used only if the HTTP response does not contain a Retry-After header.
                                       The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay. (default 60)
      --azure-retry-timeout int        Retry timeout in seconds indicates the maximum time allowed for any single try of an HTTP request.
                                       This is disabled by default. Specify a value greater than zero to enable.
                                       NOTE: Setting this to a small value might cause premature HTTP request time-outs.
```

## Unsupported flags
```bash
--machine           Output machine-readable status updates to the given path, typically a FIFO.

--no-config-file    Do not read any config file. Default: disabled

--instance          Section with these instance is read. e.g in case instance `a` is specified
                    sections cluster_a, asbackup_a is read.
  
--only-config-file  Read only this configuration file.

--s3-max-async-downloads    The maximum number of simultaneous download requests from S3.
                            The default is 32.

--s3-max-async-uploads      The maximum number of simultaneous upload requests from S3.
                            The default is 16.

--s3-log-level              The log level of the AWS S3 C++ SDK. The possible levels are,
                            from least to most granular:
                             - Off
                             - Fatal
                             - Error
                             - Warn
                             - Info
                             - Debug
                             - Trace
                            The default is Fatal.

--s3-connect-timeout        The AWS S3 client's connection timeout in milliseconds.
                            This is equivalent to cli-connect-timeout in the AWS CLI,
                            or connectTimeoutMS in the aws-sdk-cpp client configuration.
```

## Configuration file schema with default values
```yaml
app:
  # Enable more detailed logging.
  verbose: false
  # Determine log level for verbose output. Log levels are: debug, info, warn, error.
  log-level: debug
  # Set output in JSON format for parsing by external tools.
  log-json: false
cluster:
  seeds:
    - host: 127.0.0.1
      tls-name: ""
      port: 3000
  # The Aerospike user to use to connect to the Aerospike cluster.
  user: some_login
  # The Aerospike password to use to connect to the Aerospike cluster.
  password: some_password
  # The authentication mode used by the Aerospike server: INTERNAL, EXTERNAL, PKI
  auth: ""
  # Initial host connection timeout duration. The timeout when opening a connection to the server host for the first time.
  client-timeout: 30000
  # Idle timeout. Every time a connection is used,
  # its idle deadline will be extended by this duration. When this deadline is reached,
  # the connection will be closed and discarded from the connection pool.
  client-idle-timeout: 10000
  # Specifies the login operation timeout for external authentication methods such as LDAP.
  client-login-timeout: 10000
  tls:
    # TLS name used to authenticate each TLS socket connection to a database.
    name: ""
    # Set the TLS protocol selection criteria. This format is the same as Apache's SSLProtocol documented
    # at https://httpd.apache.org/docs/current/mod/mod_ssl.html#ssl protocol.
    protocols: ""
    # The CA used when connecting to Aerospike.
    ca-file: ""
    # A path containing CAs for connecting to Aerospike.
    ca-path: ""
    # The certificate file for mutual TLS authentication with Aerospike.
    cert-file: ""
    # The key file used for mutual TLS authentication with Aerospike.
    key-file: ""
    # The password used to decrypt the key file if encrypted.
    key-file-password: ""
backup:
  # The directory that holds the backup files. Required, unless `output-file` or `estimate` is used.
  directory: continue_test
  # The namespace to be backed up. Required.
  namespace: source-ns1
  # The set(s) to be backed up. Accepts comma-separated values with no spaces 'set1,set2,set3'
  # If multiple sets are being backed up, filter-exp cannot be used.
  # If empty, include all sets.
  set-list: 
   - "set1"
   - "set2"
  # Only include the given bins in the backup. Accepts comma-separated values with no spaces: 'bin1,bin2,bin3'
  # If empty include all bins.
  bin-list: 
   - "bin1"
   - "bin2"
  # Maximum number of scan calls to run in parallel.
  # If only one partition range is given, or the entire namespace is being backed up, the range
  # of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each
  # filter cannot be parallelized individually, so you may only achieve as much parallelism as there are
  # partition filters. Accepts values from 1-1024 inclusive.
  parallel: 1
  # Will not back up any records if set to true.
  no-records: false
  # Will not back up any indexes if set to true.
  no-indexes: false
  # Will not back up any UDFs if set to true.
  no-udfs: false
  # Limit total returned records per second (rps).
  # Do not apply rps limit if records-per-second is zero.
  records-per-second: 0
  # Maximum number of retries before aborting the current transaction.
  max-retries: 5
  # Total transaction timeout in milliseconds. 0 - no timeout.
  total-timeout: 0
  # Socket timeout in milliseconds. If this value is 0, it's set to total-timeout.
  # If both this and total-timeout are 0, there is no socket idle time limit.
  socket-timeout: 10000
  # The limits for read/write storage bandwidth in MiB/s.
  storage-bandwidth-limit: 0
  # Remove an existing backup file `output-file` or entire directory `directory` and replace with the new backup.
  remove-files: true
  # Remove existing backup file `output-file` or files `directory` without performing a backup.
  remove-artifacts: false
  # Backup to a single backup file. Use - for stdout. Required, unless `directory` or `estimate` is used.
  output-file: ""
  # When using directory parameter, prepend a prefix to the names of the generated files.
  output-file-prefix: ""
  # Rotate backup files when their size crosses the given value (in bytes). Only used when backing up to a directory.
  file-limit: 262144000
  # Do not include bin data in the backup. Use this flag for data sampling or troubleshooting.
  # On restore all records, that don't contain bin data will be skipped.
  no-bins: false
  # Only include records that have no ttl set (persistent records).
  no-ttl-only: false
  # Backup records after record digest in record's partition plus all succeeding
  # partitions. Used to resume backup with last record received from previous
  # incomplete backup.
  # This argument is mutually exclusive to partition-list.
  # Format: Base64 encoded string
  # Example: EjRWeJq83vEjRRI0VniavN7x
  after-digest: ""
  # <YYYY-MM-DD_HH:MM:SS>
  # Perform an incremental backup; only include records
  # that changed after the given date and time. The system's
  # local timezone applies. If only HH:MM:SS is specified, then
  # today's date is assumed as the date. If only YYYY-MM-DD is
  # specified, then 00:00:00 (midnight) is assumed as the time.
  modified-after: ""
  # <YYYY-MM-DD_HH:MM:SS>
  # Only include records that last changed before the given date and time.
  # May combined with modified-after to specify a range.
  modified-before: ""
  # Base64 encoded expression. Use the encoded filter expression in each scan call,
  # which can be used to do a partial backup. The expression to be used can be Base64
  # encoded through any client. This argument is mutually exclusive with multi-set backup.
  filter-exp: ""
  # Specifies how to perform the query of the database run for each backup.
  # By default, asbackup runs parallel workers for partitions.
  # If this flag is set to true, asbackup launches parallel workers for nodes.
  # The number of parallel workers is set by the `parallel`.
  # This option is mutually exclusive with `continue` and `estimate`.
  parallel-nodes: false
  # <addr 1>:<port 1>[,<addr 2>:<port 2>[,...]]
  # <node name 1>[,<node name 2>[,...]]
  # To get the correct node address, use 'service-tls-std' if a database configured to use TLS
  # and 'service-clear-std' info command if no TLS is configured.
  # To get the node name, use the 'node:' info command.
  # Back up the given cluster nodes only.
  # The job is parallelized by number of nodes unless `parallel` is set less than nodes number.
  # This argument is mutually exclusive with `partition-list` and `after-digest` parameters.
  # Default: backup all nodes in the cluster
  node-list:
    - "127.0.0.1:3000"
    - "127.0.0.1:3005"
  # List of partitions <filter[,<filter>[...]]> to back up. Partition filters can be ranges,
  # individual partitions, or records after a specific digest within a single partition.
  # This argument is mutually exclusive to after-digest.
  # Filter: <begin partition>[-<partition count>]|<digest>
  # begin partition: 0-4095
  # partition count: 1-4096 Default: 1
  # digest: Base64 encoded string
  # Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=
  # Default: 0-4096 (all partitions)
  partition-list: 
    - "0-1000"
  # <rack id 1>[,<rack id 2>[,...]]
  # A list of Aerospike Database rack IDs to prefer when reading records for a backup.
  prefer-racks:
    - "1"
  # <rack id 1>[,<rack id 2>[,...]]
  # A list of Aerospike Database rack IDs to backup.
  # Unlike prefer-racks, only specified racks will be backed up.
  rack-list:
    - "1"
  # The number of records approximately to back up. 0 - all records
  max-records: 0
  # The amount of milliseconds to sleep between retries after an error.
  # This field is ignored when `max-retries` is zero.
  sleep-between-retries: 5
  # Estimate the backed-up record size from a random sample of
  # 10,000 (default) records at 99.9999% confidence to estimate the full backup size.
  # It ignores any filter: filter-exp, node-list, modified-after, modified-before, no-ttl-only,
  # after-digest, partition-list
  estimate: false
  # The number of samples to take when running a backup estimate.
  estimate-samples: 10000
  # Name of a state file that will be saved in backup directory.
  # Works only with file-limit parameter. As file-limit is reached and the file is closed,
  # the current state will be saved. Works only for default and/or partition backup.
  # Not work with parallel-nodes or nodelist
  state-file-dst: ""
  # Resumes an interrupted/failed backup from where it was left off, given the .state file
  # that was generated from the interrupted/failed run.
  # continue and state-file-dst are mutually exclusive.
  continue: ""
  # Number of records will be read on one iteration for continuation backup.
  # Affects size if overlap on resuming backup after an error.
  # Used only with state-file-dst or continue.
  scan-page-size: 10000
  # How many times to retry to send info commands before failing.
  info-max-retries: 3
  # Increases the delay between subsequent retry attempts.
  # The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber)
  info-retries-multiplier: 1
  # Set the initial timeout for a retry in milliseconds when info commands are sent.
  info-retry-timeout: 1000
compression:
  # Enables compressing of backup files using the specified compression algorithm.
  # Supported compression algorithms are: zstd, none\n"+
  # Set the zstd compression level via the compression-level option.
  mode: zstd
  # zstd compression level.
  level: 3
encryption:
  # Enables encryption of backup files using the specified encryption algorithm.
  # Supported encryption algorithms are: none, aes128, aes256.
  # A private key must be given, either via the encryption-key-file option or
  # the encryption-key-env option or the encryption-key-secret.
  mode: aes128
  # Grabs the encryption key from the given file, which must be in PEM format.
  key-file: ""
  # Grabs the encryption key from the given environment variable, which must be base-64 encoded.
  key-env: ""
  # Grabs the encryption key from secret-agent.
  key-secret: ""
secret-agent:
  # Secret Agent connection type, supported types: tcp, unix.
  connection-type: tcp
  # Secret Agent host for TCP connection or socket file path for UDS connection.
  address: ""
  # Secret Agent port (only for TCP connection).
  port: 0
  # Secret Agent connection and reading timeout.
  timeout-millisecond: 0
  # Path to ca file for encrypted connections.
  ca-file: ""
  # Whether Secret Agent responses are Base64 encoded.
  is-base64: false
aws:
  s3:
    # Existing S3 bucket name
    bucket-name: ""
    # The S3 region that the bucket(s) exist in.
    region: ""
    # The S3 profile to use for credentials.
    profile: ""
    # An alternate url endpoint to send S3 API calls to.
    endpoint-override: ""
    # S3 access key id. If not set, profile auth info will be used.
    access-key-id: ""
    # S3 secret access key. If not set, profile auth info will be used.
    secret-access-key: ""
    # Apply storage class to backup files. Storage classes are:
    # STANDARD,
    # REDUCED_REDUNDANCY,
    # STANDARD_IA,
    # ONEZONE_IA,
    # INTELLIGENT_TIERING,
    # GLACIER,
    # DEEP_ARCHIVE,
    # OUTPOSTS,
    # GLACIER_IR,
    # SNOW,
    # EXPRESS_ONEZONE
    storage-class: ""
    # Maximum number of attempts that should be made in case of an error.
    retry-max-attempts: 100
    # Max backoff duration in seconds between retried attempts.
    retry-max-backoff: 90
    # Provides the backoff in seconds strategy the retryer will use to determine the delay between retry attempts.
    retry-backoff: 60
    # Chunk size controls the maximum number of bytes of the object that the app will attempt to send to
    # the storage in a single request. Objects smaller than the size will be sent in a single request,
    # while larger objects will be split over multiple requests
    chunk-size: 5242880
gcp:
  storage:
    # Path to file containing service account JSON key.
    key-file: ""
    # Name of the Google cloud storage bucket.
    bucket-name: ""
    # An alternate url endpoint to send GCP API calls to.
    endpoint-override: ""
    # Max retries specifies the maximum number of attempts a failed operation will be retried before producing an error.
    retry-max-attempts: 100
    # Max backoff is the maximum value in seconds of the retry period.
    retry-max-backoff: 90
    # Initial backoff is the initial value in seconds of the retry period.
    retry-init-backoff: 60
    # Multiplier is the factor by which the retry period increases. It should be greater than 1.
    retry-backoff-multiplier: 2
    # Chunk size controls the maximum number of bytes of the object that the app will attempt to send to
    # the storage in a single request. Objects smaller than the size will be sent in a single request,
    # while larger objects will be split over multiple requests.
    chunk-size: 5242880
azure:
  blob:
    # Azure account name for account name, key authorization.
    account-name: ""
    # Azure account key for account name, key authorization.
    account-key: ""
    # Azure tenant ID for Azure Active Directory authorization.
    tenant-id: ""
    # Azure client ID for Azure Active Directory authorization.
    client-id: ""
    # Azure client secret for Azure Active Directory authorization.
    client-secret: ""
    # Azure endpoint.
    endpoint-override: ""
    # Azure container Name.
    container-name: ""
    # Tiers are: Archive, Cold, Cool, Hot, P10, P15, P20, P30, P4, P40, P50, P6, P60, P70, P80, Premium.
    access-tier: ""
    # Max retries specifies the maximum number of attempts a failed operation will be retried before producing an error.
    retry-max-attempts: 100
    # Retry timeout in seconds indicates the maximum time allowed for any single try of an HTTP request.
    # This is disabled by default. Specify a value greater than zero to enable.
    # NOTE: Setting this to a small value might cause premature HTTP request time-outs.
    retry-timeout: 10
    # Retry delay specifies the initial amount of delay in seconds to use before retrying an operation.
    # The value is used only if the HTTP response does not contain a Retry-After header.
    # The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay.
    retry-delay: 60
    # Max retry delay specifies the maximum delay in seconds allowed before retrying an operation.
    # Typically the value is greater than or equal to the value specified in azure-retry-delay.
    retry-max-delay: 90
    # Block size defines the size of the buffer used during upload.
    block-size: 5242880
```