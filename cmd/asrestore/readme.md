# ASBackup
Aerospike Restore CLI tool.

## Building
### Dev build
```bash
make build
```
### Release build
```bash
# for local build
make release-test
# for release
make release
```

###  Release requirements.
For creating releases, you must have `goreleaser` installed on your device.
```bash
brew install goreleaser/tap/goreleaser
```
Configure `GITHUB_TOKEN`, `GITLAB_TOKEN` and `GITEA_TOKEN`.
More info here https://goreleaser.com/quick-start/

## Supported flags
```
Usage:
  asrestore [flags]

General Flags:
  -Z, --help      Display help information.
  -V, --version   Display version information.
  -v, --verbose   Enable more detailed logging.

Aerospike Client Flags:
  -h, --host host[:tls-name][:port][,...]                                                           The Aerospike host. (default 127.0.0.1)
  -p, --port int                                                                                    The default Aerospike port. (default 3000)
  -U, --user string                                                                                 The Aerospike user to use to connect to the Aerospike cluster.
  -P, --password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"                   The Aerospike password to use to connect to the Aerospike cluster.
      --auth INTERNAL,EXTERNAL,PKI                                                                  The authentication mode used by the Aerospike server. INTERNAL uses standard user/pass. EXTERNAL uses external methods (like LDAP) which are configured on the server. EXTERNAL requires TLS. PKI allows TLS authentication and authorization based on a certificate. No user name needs to be configured. (default INTERNAL)
      --tls-enable                                                                                  Enable TLS authentication with Aerospike. If false, other tls options are ignored.
      --tls-name string                                                                             The server TLS context to use to authenticate the connection to Aerospike.
      --tls-cafile env-b64:<cert>,b64:<cert>,<cert-file-name>                                       The CA used when connecting to Aerospike.
      --tls-capath <cert-path-name>                                                                 A path containing CAs for connecting to Aerospike.
      --tls-certfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                     The certificate file for mutual TLS authentication with Aerospike.
      --tls-keyfile env-b64:<cert>,b64:<cert>,<cert-file-name>                                      The key file used for mutual TLS authentication with Aerospike.
      --tls-keyfile-password "env-b64:<env-var>,b64:<b64-pass>,file:<pass-file>,<clear-pass>"       The password used to decrypt the key-file if encrypted.
      --tls-protocols "[[+][-]all] [[+][-]TLSv1] [[+][-]TLSv1.1] [[+][-]TLSv1.2] [[+][-]TLSv1.3]"   Set the TLS protocol selection criteria. This format is the same as Apache's SSLProtocol documented at https://httpd.apache.org/docs/current/mod/mod_ssl.html#ssl protocol. (default +TLSv1.2)

Restore Flags:
  -d, --directory string         The Directory that holds the backup files. Required, unless -o or -e is used.
  -n, --namespace string         The namespace to be backed up. Required.
  -s, --set stringArray          The set(s) to be backed up.
                                 If multiple sets are being backed up, filter-exp cannot be used.
                                 Default: all sets.
  -L, --records-per-second int   Limit total returned records per second (rps).
                                 Do not apply rps limit if records-per-second is zero.
                                 Default: 0.
  -B, --bin-list stringArray     Only include the given bins in the backup.
                                 Default: include all bins.
  -w, --parallel int             Maximum number of scan calls to run in parallel.
                                 If only one partition range is given, or the entire namespace is being backed up, the range
                                 of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each
                                 filter cannot be parallelized individually, so you may only achieve as much parallelism as there are
                                 partition filters.
                                 Default: 1 (default 1)
  -R, --no-records               Don't backup any records.
  -I, --no-indexes               Don't backup any indexes.
      --no-udfs                  Don't backup any UDFs.
      --max-retries int          Maximum number of retries before aborting the current transaction.
                                 Default: 5 (default 5)
      --total-timeout int        Total socket timeout in milliseconds.
                                 Default: 0 - no timeout.
      --socket-timeout int       Socket timeout in milliseconds. If this value is 0, its set to total-timeout. If both are 0,
                                 there is no socket idle time limit
                                 Default: 10 seconds. (default 10000)
  -N, --nice int                 The limits for read/write storage bandwidth in MiB/s
  -i, --input-file string        Restore from a single backup file. Use - for stdin.
                                 Required, unless --directory or --directory-list is used.
                                 
      --ignore-record-error      Ignore permanent record specific error. e.g AEROSPIKE_RECORD_TOO_BIG.
                                 By default such errors are not ignored and asrestore terminates.
                                 Optional: Use verbose mode to see errors in detail.
      --disable-batch-writes     Disables the use of batch writes when restoring records to the Aerospike cluster.
                                 By default, the cluster is checked for batch write support, so only set this flag if you explicitly
                                 don't want
                                 batch writes to be used or asrestore is failing to recognize that batch writes are disabled
                                 and is failing to work because of it.
      --max-async-batches int    The max number of outstanding async record batch write calls at a time.
                                 For pre-6.0 servers, 'batches' are only a logical grouping of
                                 records, and each record is uploaded individually. The true max
                                 number of async aerospike calls would then be
                                 <max-async-batches> * <batch-size>.
                                 Default: 32 (default 32)
      --batch-size int           The max allowed number of records to simultaneously upload
                                 in an async batch write calls to make to aerospike at a time.
                                 Default is 128 with batch writes enabled, or 16 without batch writes. (default 128)
      --extra-ttl int            For records with expirable void-times, add N seconds of extra-ttl to the
                                 recorded void-time.
  -u, --unique                   Skip records that already exist in the namespace;
                                 Don't touch them.
                                 
  -r, --replace                  Fully replace records that already exist in the namespace;
                                 Don't update them.
                                 
  -g, --no-generation            Don't check the generation of records that already exist in the namespace.
  -T, --timeout int              Set the timeout (ms) for commands. 
                                 Default: 10000 (default 10000)
      --retry-base-timeout int   Set the initial delay between retry attempts in milliseconds
                                 Default: 10000 (default 10000)
      --retry-multiplier float   retry-multiplier is used to increase the delay between subsequent retry attempts.
                                 The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber)
                                 Default: 0
      --retry-max-retries uint   Set the maximum number of retry attempts that will be made. If set to 0, no retries will be performed.
                                 Default: 0

Compression Flags:
  -z, --compress string         Enables compressing of backup files using the specified compression algorithm.
                                Supported compression algorithms are: zstd, none
                                Set the zstd compression level via the --compression-level option. Default level is 3. (default "NONE")
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
      --sa-connection-type string   Secret agent connection type, supported types: tcp, unix. (default "tcp")
      --sa-address string           Secret agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret agent port (only for TCP connection).
      --sa-timeout int              Secret agent connection and reading timeout.
      --sa-cafile string            Path to ca file for encrypted connection.
      --sa-is-base64                Flag that shows if secret agent responses are encrypted with base64.

AWS Flags:
      --s3-region string              The S3 region that the bucket(s) exist in.
      --s3-profile string             The S3 profile to use for credentials (the default is 'default'). (default "default")
      --s3-endpoint-override string   An alternate url endpoint to send S3 API calls to.
      --s3-min-part-size int          Secret agent port (only for TCP connection). (default 3005)

GCP Flags:
      --gcp-key-path string            Path to file containing Service Account JSON Key.
      --gcp-bucket-name string         Name of the Google Cloud Storage bucket.
      --gcp-endpoint-override string   An alternate url endpoint to send GCP API calls to.

Azure Flags:
      --azure-account-name string     Azure account name for account name, key authorization.
      --azure-account-key string      Azure account key for account name, key authorization.
      --azure-tenant-id string        Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string        Azure Client ID for Azure Active Directory authorization.
      --azure-client-secret string    Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string         Azure endpoint.
      --azure-container-name string   Azure container Name.
```

## Unsupported flags
- azure blob connection flags
- gcp storage connection flags
```
--directory-list        A comma seperated list of paths to directories that hold the backup files. Required,
                        unless -i or -d is used. The paths may not contain commas
                        Example: `asrestore --directory-list /path/to/dir1/,/path/to/dir2
                        
--parent-directory      A common root path for all paths used in --directory-list.
                        This path is prepended to all entries in --directory-list.
                        Example: `asrestore --parent-directory /common/root/path --directory-list /path/to/dir1/,/path/to/dir2

-m, --machine <path>    Output machine-readable status updates to the given path, 
                        typically a FIFO.
                        
--validate      Validate backup files but don't restore anything.

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
