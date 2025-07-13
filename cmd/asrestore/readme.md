# Aerospike restore (asrestore)
Aerospike Restore CLI tool. This page describes capabilities and configuration options of the Aerospike restore tool, `asrestore`.

## Overview
`asrestore` restores backups created with `asbackup`. With the `asrestore` tool, you can restore to specific bins or sets, secure connections using username/password credentials or TLS (or both), and use configuration files to automate restore operations.

## Considerations for Aerospike restore
When using `asrestore`, be aware of the following considerations:

- The TTL of restored keys is preserved, but the last-update-time and generation count are reset to the current time.
- `asrestore` creates records from the backup. If records exist in the namespace on the cluster, you can configure a write policy to determine whether the backup records or the records in the namespace take precedence when using `asrestore`.
- If a restore transaction fails, you can configure timeout options for retries.
- Restore is cluster-configuration-agnostic. A backup can be restored to a cluster of any size and configuration. Restored data is evenly distributed among cluster nodes, regardless of cluster configuration.

## Privileges required for `asrestore`
The privileges required to run `asrestore` depend on the type of objects in the namespace.

- If the namespace does not contain [user-defined functions](https://aerospike.com/docs/database/learn/architecture/udf) or [secondary indexes](https://aerospike.com/docs/database/learn/architecture/data-storage/secondary-index), `read-write` is the minimum necessary privilege.
- If the namespace contains [user-defined functions](https://aerospike.com/docs/database/learn/architecture/udf), `udf-admin` is the minimum necessary privilege to restore UDFs for Database 6.0 or newer. Otherwise, use `data-admin`.
- If the namespace contains [secondary indexes](https://aerospike.com/docs/database/learn/architecture/data-storage/secondary-index), `sindex-admin` is the minimum necessary privilege to restore secondary indexes for Database 6.0 or newer. Otherwise, use `data-admin`.

For more information about Aerospike’s role-based access control system, see [Configuring Access Control in EE and FE](https://aerospike.com/docs/database/manage/security/rbac/#privileges).

---

## Build
### Dev
```bash
make build
```
### Release
Version artifacts are automatically built and uploaded under releases in GitHub.

## Supported flags
```
Usage:
  asrestore [flags]

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

Restore Flags:
  -d, --directory string         The directory that holds the backup files. Required, unless --input-file is used.
  -n, --namespace string         Used to restore to a different namespace. Example: source-ns,destination-ns
                                 Restoring to different namespace is incompatible with --mode=asbx.
  -s, --set string               Only restore the given sets from the backup.
                                 Default: restore all sets.
                                 Incompatible with --mode=asbx.
  -B, --bin-list string          Only restore the given bins in the backup.
                                 If empty, include all bins.
                                 Incompatible with --mode=asbx.
  -R, --no-records               Don't restore any records.
                                 Incompatible with --mode=asbx.
  -I, --no-indexes               Don't restore any secondary indexes.
                                 Incompatible with --mode=asbx.
      --no-udfs                  Don't restore any UDFs.
                                 Incompatible with --mode=asbx.
  -w, --parallel int             The number of restore threads. Accepts values from 1-1024 inclusive.
                                 If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.
  -L, --records-per-second int   Limit total returned records per second (rps).
                                 Do not apply rps limit if records-per-second is zero.
      --max-retries int          Maximum number of retries before aborting the current transaction. (default 5)
      --total-timeout int        Total transaction timeout in milliseconds. 0 - no timeout. (default 10000)
      --socket-timeout int       Socket timeout in milliseconds. If this value is 0, it's set to --total-timeout.
                                 If both this and --total-timeout are 0, there is no socket idle time limit. (default 10000)
  -N, --nice int                 The limits for read/write storage bandwidth in MiB/s
                                 The lower bound is 8MiB (maximum size of the Aerospike record). Default is 0 (no limit).
  -i, --input-file string         Restore from a single backup file. Use - for stdin.
                                  Required, unless --directory or --directory-list is used.
                                  Incompatible with --mode=asbx.
      --directory-list string     A comma-separated list of paths to directories that hold the backup files. Required,
                                  unless -i or -d is used. The paths may not contain commas.
                                  Example: 'asrestore --directory-list /path/to/dir1/,/path/to/dir2'
                                  Incompatible with --mode=asbx.
      --parent-directory string   A common root path for all paths used in --directory-list.
                                  This path is prepended to all entries in --directory-list.
                                  Example: 'asrestore --parent-directory /common/root/path
                                  --directory-list /path/to/dir1/,/path/to/dir2'
                                  Incompatible with --mode=asbx.
  -u, --unique                    Skip modifying records that already exist in the namespace.
  -r, --replace                   Fully replace records that already exist in the namespace.
                                  This option still performs a generation check by default and needs to be combined with the -g option
                                  if you do not want to perform a generation check.
                                  This option is mutually exclusive with --unique.
  -g, --no-generation             Don't check the generation of records that already exist in the namespace.
      --ignore-record-error       Ignore errors specific to records, not UDFs or indexes. The errors are:
                                  AEROSPIKE_RECORD_TOO_BIG,
                                  AEROSPIKE_KEY_MISMATCH,
                                  AEROSPIKE_BIN_NAME_TOO_LONG,
                                  AEROSPIKE_ALWAYS_FORBIDDEN,
                                  AEROSPIKE_FAIL_FORBIDDEN,
                                  AEROSPIKE_BIN_TYPE_ERROR,
                                  AEROSPIKE_BIN_NOT_FOUND.
                                  By default, these errors are not ignored and asrestore terminates.
      --disable-batch-writes      Disables the use of batch writes when restoring records to the Aerospike cluster.
                                  By default, the cluster is checked for batch write support. Only set this flag if you explicitly
                                  don't want batch writes to be used or if asrestore is failing to work because it cannot recognize
                                  that batch writes are disabled.
                                  Incompatible with --mode=asbx.
      --max-async-batches int     To send data to Aerospike Database, asrestore creates write workers that work in parallel.
                                  This value is the number of workers that form batches and send them to the database.
                                  For Aerospike Database versions prior to 6.0, 'batches' are only a logical grouping of records,
                                  and each record is uploaded individually.
                                  The true max number of async Aerospike calls would then be <max-async-batches> * <batch-size>.
                                  Incompatible with --mode=asbx. (default 32)
      --warm-up int               Warm Up fills the connection pool with connections for all nodes. This is necessary for batch restore.
                                  By default is calculated as (--max-async-batches + 1), as one connection per node is reserved
                                  for tend operations and is not used for transactions.
                                  Incompatible with --mode=asbx.
      --batch-size int            The max allowed number of records to simultaneously upload to Aerospike.
                                  Default is 128 with batch writes enabled. If you disable batch writes,
                                  this flag is superseded because each worker sends writes one by one.
                                  All three batch flags are linked. If --disable-batch-writes=false,
                                  asrestore uses batch write workers to send data to the database.
                                  Asrestore creates a number of workers equal to --max-async-batches that work in parallel,
                                  and form and send a number of records equal to --batch-size to the database.
                                  Incompatible with --mode=asbx. (default 128)
      --extra-ttl int             For records with expirable void-times, add N seconds of extra-ttl to the
                                  recorded void-time.
                                  Incompatible with --mode=asbx.
  -T, --timeout int               Set the timeout (ms) for asinfo commands sent from asrestore to the database.
                                  The info commands are to check version, get indexes, get udfs, count records, and check batch write support. (default 10000)
      --retry-base-timeout int    Set the initial timeout for a retry in milliseconds when data is sent to the Aerospike database
                                  during a restore. This retry sequence is triggered by the following non-critical errors:
                                  AEROSPIKE_NO_AVAILABLE_CONNECTIONS_TO_NODE,
                                  AEROSPIKE_TIMEOUT,
                                  AEROSPIKE_DEVICE_OVERLOAD,
                                  AEROSPIKE_NETWORK_ERROR,
                                  AEROSPIKE_SERVER_NOT_AVAILABLE,
                                  AEROSPIKE_BATCH_FAILED,
                                  AEROSPIKE_MAX_ERROR_RATE.
                                  This base timeout value is also used as the interval multiplied by --retry-multiplier to increase
                                  the timeout value between retry attempts. (default 1000)
      --retry-multiplier float    Increases the delay between subsequent retry attempts for the errors listed under --retry-base-timeout.
                                  The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber) (default 1)
      --retry-max-retries uint    Set the maximum number of retry attempts for the errors listed under --retry-base-timeout.
                                  The default is 0, indicating no retries will be performed
      --mode string               Restore mode: auto, asb, asbx. According to this parameter different restore processes wil be started.
                                  auto - starts restoring from both .asb and .asbx files.
                                  asb - restore only .asb backup files.
                                  asbx - restore only .asbx backup files. (default "auto")
      --validate                  Validate backup files without restoring.

Compression Flags:
  -z, --compress string         Enables decompressing of backup files using the specified compression algorithm.
                                This must match the compression mode used when backing up the data.
                                Supported compression algorithms are: zstd, none
                                Set the zstd compression level via the --compression-level option. (default "NONE")
      --compression-level int   zstd compression level. (default 3)

Encryption Flags:
      --encrypt string                 Enables decryption of backup files using the specified encryption algorithm.
                                       This must match the encryption mode used when backing up the data.
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
      --s3-bucket-name string          Existing S3 bucket name
      --s3-region string               The S3 region that the bucket(s) exist in.
      --s3-profile string              The S3 profile to use for credentials.
      --s3-access-key-id string        S3 access key id. If not set, profile auth info will be used.
      --s3-secret-access-key string    S3 secret access key. If not set, profile auth info will be used.
      --s3-endpoint-override string    An alternate url endpoint to send S3 API calls to.
      --s3-tier string                 If is set, tool will try to restore archived files to the specified tier.
                                       Tiers are: Standard, Bulk, Expedited.
      --s3-restore-poll-duration int   How often (in milliseconds) a backup client checks object status when restoring an archived object. (default 60000)
      --s3-retry-max-attempts int      Maximum number of attempts that should be made in case of an error. (default 100)
      --s3-retry-max-backoff int       Max backoff duration in seconds between retried attempts. (default 90)
      --s3-retry-backoff int           Provides the backoff in seconds strategy the retryer will use to determine the delay between retry attempts. (default 60)

GCP Flags:
For GCP storage bucket name is mandatory, and is set with --gcp-bucket-name flag.
So --directory path will only contain folder name.
Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.
Any GCP parameter can be retrieved from Secret Agent.
      --gcp-key-path string                  Path to file containing service account JSON key.
      --gcp-bucket-name string               Name of the Google cloud storage bucket.
      --gcp-endpoint-override string         An alternate url endpoint to send GCP API calls to.
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
      --azure-account-name string           Azure account name for account name, key authorization.
      --azure-account-key string            Azure account key for account name, key authorization.
      --azure-tenant-id string              Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string              Azure client ID for Azure Active Directory authorization.
      --azure-client-secret string          Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string               Azure endpoint.
      --azure-container-name string         Azure container Name.
      --azure-access-tier string            If is set, tool will try to rehydrate archived files to the specified tier.
                                            Tiers are: Archive, Cold, Cool, Hot, P10, P15, P20, P30, P4, P40, P50, P6, P60, P70, P80, Premium.
      --azure-rehydrate-poll-duration int   How often (in milliseconds) a backup client checks object status when restoring an archived object. (default 60000)
      --azure-retry-max-attempts int        Max retries specifies the maximum number of attempts a failed operation will be retried
                                            before producing an error. (default 100)
      --azure-retry-max-delay int           Max retry delay specifies the maximum delay in seconds allowed before retrying an operation.
                                            Typically the value is greater than or equal to the value specified in azure-retry-delay. (default 90)
      --azure-retry-delay int               Retry delay specifies the initial amount of delay in seconds to use before retrying an operation.
                                            The value is used only if the HTTP response does not contain a Retry-After header.
                                            The delay increases exponentially with each retry up to the maximum specified by azure-retry-max-delay. (default 60)
      --azure-retry-timeout int             Retry timeout in seconds indicates the maximum time allowed for any single try of an HTTP request.
                                            This is disabled by default. Specify a value greater than zero to enable.
                                            NOTE: Setting this to a small value might cause premature HTTP request time-outs.
```

## Unsupported flags
```

-m, --machine <path>    Output machine-readable status updates to the given path, 
                        typically a FIFO.

--indexes-last  Restore secondary indexes only after UDFs and records have been restored.

--wait          Wait for restored secondary indexes to finish building. 
                Wait for restored UDFs to be distributed across the cluster.

// Replaced with:
//  --retry-base-timeout
//  --retry-multiplier
//  --retry-max-retries
--retry-scale-factor        The scale factor to use in the exponential backoff retry
                            strategy, in microseconds.
                            Default is 150000 us (150 ms).
                            
--event-loops               The number of c-client event loops to initialize for
                            processing of asynchronous Aerospike transactions.
                            Default is 1.

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


## Config explanation
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
  # Enable TLS authentication with Aerospike.
  tls-enable: false
  # The server TLS context to use to authenticate the connection to Aerospike.
  tls-name: ""
  # Set the TLS protocol selection criteria. This format is the same as Apache's SSLProtocol documented
  # at https://httpd.apache.org/docs/current/mod/mod_ssl.html#ssl protocol.
  tls-protocols: ""
  # The CA used when connecting to Aerospike.
  tls-ca-file: ""
  # A path containing CAs for connecting to Aerospike.
  tls-ca-path: ""
  # The certificate file for mutual TLS authentication with Aerospike.
  tls-cert-file: ""
  # The key file used for mutual TLS authentication with Aerospike.
  tls-key-file: ""
  # The password used to decrypt the key file if encrypted.
  tls-key-file-password: ""
client-policy:
  # Initial host connection timeout duration. The timeout when opening a connection to the server host for the first time.
  timeout: 30000
  # Idle timeout. Every time a connection is used,
  # its idle deadline will be extended by this duration. When this deadline is reached,
  # the connection will be closed and discarded from the connection pool.
  idle-timeout: 10000
  # Specifies the login operation timeout for external authentication methods such as LDAP.
  login-timeout: 10000
restore:
  # The directory that holds the backup files. Required, unless --input-file is used.
  directory: continue_test
  # Used to restore to a different namespace. Example: source-ns,destination-ns
  # Restoring to different namespace is incompatible with --mode=asbx.
  namespace: source-ns1
  # Only restore the given sets from the backup.
  # Default: restore all sets.
  # Incompatible with --mode=asbx.
  set-list: set1,set2
  # Only restore the given bins in the backup.
  # If empty, include all bins.
  # Incompatible with --mode=asbx.
  bin-list: bin1,bin2
  # The number of restore threads. Accepts values from 1-1024 inclusive.
  # If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.
  parallel: 1
  # Don't restore any records.
  no-records: false
  # Don't restore any indexes.
  no-indexes: false
  # Don't restore any UDFs.
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
  nice: 0
  # Restore from a single backup file. Use - for stdin.
  # Required, unless --directory or --directory-list is used.
  # Incompatible with --mode=asbx.
  input-file: ""
  # A comma-separated list of paths to directories that hold the backup files. Required,
  # unless -i or -d is used. The paths may not contain commas.
  # Example: 'asrestore --directory-list /path/to/dir1/,/path/to/dir2'
  # Incompatible with --mode=asbx.
  directory-list: ""
  # A common root path for all paths used in --directory-list.
  # This path is prepended to all entries in --directory-list.
  # Example: 'asrestore --parent-directory /common/root/path
  # --directory-list /path/to/dir1/,/path/to/dir2'
  # Incompatible with --mode=asbx.
  parent-directory: ""
  # Disables the use of batch writes when restoring records to the Aerospike cluster.
  # By default, the cluster is checked for batch write support. Only set this flag if you explicitly
  # don't want batch writes to be used or if asrestore is failing to work because it cannot recognize
  # that batch writes are disabled.
  # Incompatible with --mode=asbx.
  disable-batch-writes: false
  # The max allowed number of records to simultaneously upload to Aerospike.
  # Default is 128 with batch writes enabled. If you disable batch writes,
  # this flag is superseded because each worker sends writes one by one.
  # All three batch flags are linked. If --disable-batch-writes=false,
  # asrestore uses batch write workers to send data to the database.
  # Asrestore creates a number of workers equal to --max-async-batches that work in parallel,
  # and form and send a number of records equal to --batch-size to the database.
  # Incompatible with --mode=asbx.
  batch-size: 128
  # To send data to Aerospike Database, asrestore creates write workers that work in parallel.
  # This value is the number of workers that form batches and send them to the database.
  # For Aerospike Database versions prior to 6.0, 'batches' are only a logical grouping of records,
  # and each record is uploaded individually.
  # The true max number of async Aerospike calls would then be <max-async-batches> * <batch-size>.
  # Incompatible with --mode=asbx.
  max-async-batches: 32
  # Warm Up fills the connection pool with connections for all nodes. This is necessary for batch restore.
  # By default is calculated as (--max-async-batches + 1), as one connection per node is reserved
  # for tend operations and is not used for transactions.
  # Incompatible with --mode=asbx.
  warm-up: 0
  # For records with expirable void-times, add N seconds of extra-ttl to the
  # recorded void-time.
  # Incompatible with --mode=asbx.
  extra-ttl: 0
  # Ignore errors specific to records, not UDFs or indexes. The errors are:
  # AEROSPIKE_RECORD_TOO_BIG,
  # AEROSPIKE_KEY_MISMATCH,
  # AEROSPIKE_BIN_NAME_TOO_LONG,
  # AEROSPIKE_ALWAYS_FORBIDDEN,
  # AEROSPIKE_FAIL_FORBIDDEN,
  # AEROSPIKE_BIN_TYPE_ERROR,
  # AEROSPIKE_BIN_NOT_FOUND.
  # By default, these errors are not ignored and asrestore terminates.
  ignore-record-error: false
  # Skip modifying records that already exist in the namespace.
  uniq: false
  # Fully replace records that already exist in the namespace.
  # This option still performs a generation check by default and needs to be combined with the -g option
  # if you do not want to perform a generation check.
  # This option is mutually exclusive with --unique.
  replace: false
  # Don't check the generation of records that already exist in the namespace.
  no-generation: false
  # Set the timeout (ms) for asinfo commands sent from asrestore to the database.
  # The info commands are to check version, get indexes, get udfs, count records, and check batch write support.
  timeout: 10000
  # Set the initial timeout for a retry in milliseconds when data is sent to the Aerospike database
  # during a restore. This retry sequence is triggered by the following non-critical errors:
  # AEROSPIKE_NO_AVAILABLE_CONNECTIONS_TO_NODE,
  # AEROSPIKE_TIMEOUT,
  # AEROSPIKE_DEVICE_OVERLOAD,
  # AEROSPIKE_NETWORK_ERROR,
  # AEROSPIKE_SERVER_NOT_AVAILABLE,
  # AEROSPIKE_BATCH_FAILED,
  # AEROSPIKE_MAX_ERROR_RATE.
  # This base timeout value is also used as the interval multiplied by --retry-multiplier to increase
  # the timeout value between retry attempts.
  retry-base-timeout: 1000
  # Increases the delay between subsequent retry attempts for the errors listed under --retry-base-timeout.
  # The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber)
  retry-multiplier: 1
  # Set the maximum number of retry attempts for the errors listed under --retry-base-timeout.
  # The default is 0, indicating no retries will be performed
  retry-max-retries: 1
  # Restore mode: auto, asb, asbx. According to this parameter different restore processes wil be started.
  # auto - starts restoring from both .asb and .asbx files.
  # asb - restore only .asb backup files.
  # asbx - restore only .asbx backup files.
  mode: "auto"
  # Validate backup files without restoring.
  validate-only: false

compression:
  # Enables decompressing of backup files using the specified compression algorithm.
  # This must match the compression mode used when backing up the data.
  # Supported compression algorithms are: zstd, none
  # Set the zstd compression level via the compression-level option.
  mode: NONE
  # zstd compression level.
  level: 3
encryption:
  # Enables decryption of backup files using the specified encryption algorithm.
  # This must match the encryption mode used when backing up the data.
  # Supported encryption algorithms are: none, aes128, aes256.
  # A private key must be given, either via the encryption-key-file option or
  # the encryption-key-env option or the encryption-key-secret.
  mode: none
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
  # If is set, tool will try to restore archived files to the specified tier.
  # Tiers are: Standard, Bulk, Expedited.
  access-tier: ""
  # How often (in milliseconds) a backup client checks object status when restoring an archived object.
  restore-poll-duration: 1000
  # Maximum number of attempts that should be made in case of an error.
  retry-max-attempts: 100
  # Max backoff duration in seconds between retried attempts.
  retry-max-backoff: 90
  # Provides the backoff in seconds strategy the retryer will use to determine the delay between retry attempts.
  retry-backoff: 60
gcp:
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
azure:
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
  rehydrate-poll-duration: 100
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
```