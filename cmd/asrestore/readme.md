# ASRestore
Aerospike Restore CLI tool.

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
                                   
      --client-login-timeout int   specifies the timeout for login operation for external authentication such as LDAP. (default 10000)

Restore Flags:
  -d, --directory string         The Directory that holds the backup files. Required, unless -o or -e is used.
  -n, --namespace string         Used to restore to a different namespace. Example: source-ns,destination-ns
  -s, --set string               Only restore the given sets from the backup.
                                 Default: restore all sets.
  -B, --bin-list string          Only restore the given bins in the backup.
                                 Default: restore all bins.
                                 
  -R, --no-records               Don't restore any records.
  -I, --no-indexes               Don't restore any secondary indexes.
      --no-udfs                  Don't restore any UDFs.
  -w, --parallel int             The number of restore threads. (default - the number of available CPUs)
  -L, --records-per-second int   Limit total returned records per second (rps).
                                 Do not apply rps limit if records-per-second is zero.
      --max-retries int          Maximum number of retries before aborting the current transaction. (default 5)
      --total-timeout int        Total transaction timeout in milliseconds. 0 - no timeout. (default 10000)
      --socket-timeout int       Socket timeout in milliseconds. If this value is 0, it's set to total-timeout. If both are 0,
                                 there is no socket idle time limit (default 10000)
  -N, --nice int                 The limits for read/write storage bandwidth in MiB/s
  -i, --input-file string         Restore from a single backup file. Use - for stdin.
                                  Required, unless --directory or --directory-list is used.
                                  
      --directory-list string     A comma separated list of paths to directories that hold the backup files. Required,
                                  unless -i or -d is used. The paths may not contain commas
                                  Example: `asrestore --directory-list /path/to/dir1/,/path/to/dir2
      --parent-directory string   A common root path for all paths used in --directory-list.
                                  This path is prepended to all entries in --directory-list.
                                  Example: `asrestore --parent-directory /common/root/path --directory-list /path/to/dir1/,/path/to/dir2
  -u, --unique                    Skip records that already exist in the namespace;
                                  Don't touch them.
                                  
  -r, --replace                   Fully replace records that already exist in the namespace.
                                  Generation check is conducted during replace.
                                  This option still does a generation check by default and would need to be combined with the -g option 
                                  if no generation check is desired. 
                                  Note: this option is mutually exclusive to --unique.
  -g, --no-generation             Don't check the generation of records that already exist in the namespace.
      --ignore-record-error       Ignore permanent record specific error. e.g AEROSPIKE_RECORD_TOO_BIG.
                                  By default such errors are not ignored and asrestore terminates.
                                  Optional: Use verbose mode to see errors in detail.
      --disable-batch-writes      Disables the use of batch writes when restoring records to the Aerospike cluster.
                                  By default, the cluster is checked for batch write support, so only set this flag if you explicitly
                                  don't want
                                  batch writes to be used or asrestore is failing to recognize that batch writes are disabled
                                  and is failing to work because of it.
      --max-async-batches int     The max number of outstanding async record batch write calls at a time.
                                  For pre-6.0 servers, 'batches' are only a logical grouping of
                                  records, and each record is uploaded individually. The true max
                                  number of async aerospike calls would then be
                                  <max-async-batches> * <batch-size>. (default 32)
      --batch-size int            The max allowed number of records to simultaneously upload
                                  in an async batch write calls to make to aerospike at a time.
                                  Default is 128 with batch writes enabled, or 16 without batch writes. (default 128)
      --extra-ttl int             For records with expirable void-times, add N seconds of extra-ttl to the
                                  recorded void-time.
  -T, --timeout int               Set the timeout (ms) for info commands. (default 10000)
      --retry-base-timeout int    Set the initial delay between retry attempts in milliseconds (default 1000)
      --retry-multiplier float    retry-multiplier is used to increase the delay between subsequent retry attempts.
                                  The actual delay is calculated as: retry-base-timeout * (retry-multiplier ^ attemptNumber) (default 1)
      --retry-max-retries uint    Set the maximum number of retry attempts that will be made. If set to 0, no retries will be performed.

Compression Flags:
  -z, --compress string         Enables decompressing of backup files using the specified compression algorithm.
                                This must match the compression mode used when backing up the data.
                                Supported compression algorithms are: zstd, none
                                Set the zstd compression level via the --compression-level option. Default level is 3. (default "NONE")
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
Options pertaining to the Aerospike secret agent https://docs.aerospike.com/tools/secret-agent.
Both asbackup and asrestore support getting all the cloud config parameters from the Aerospike secret agent.
To use a secret as an option, use this format 'secrets:<resource_name>:<secret_name>' 
Example: asrestore --azure-account-name secret:resource1:azaccount
      --sa-connection-type string   Secret agent connection type, supported types: tcp, unix. (default "tcp")
      --sa-address string           Secret agent host for TCP connection or socket file path for UDS connection.
      --sa-port int                 Secret agent port (only for TCP connection).
      --sa-timeout int              Secret agent connection and reading timeout.
      --sa-cafile string            Path to ca file for encrypted connection.
      --sa-is-base64                Flag that shows if secret agent responses are encrypted with base64.

AWS Flags:
For S3 storage bucket name is mandatory, and is set with --s3-bucket-name flag.
So --directory path will only contain folder name.
--s3-endpoint-override is used in case you want to use minio, instead of AWS.
Any AWS parameter can be retrieved from secret agent.
      --s3-bucket-name string         Existing S3 bucket name
      --s3-region string              The S3 region that the bucket(s) exist in.
      --s3-profile string             The S3 profile to use for credentials.
      --s3-endpoint-override string   An alternate url endpoint to send S3 API calls to.

GCP Flags:
For GCP storage bucket name is mandatory, and is set with --gcp-bucket-name flag.
So --directory path will only contain folder name.
Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.
Any GCP parameter can be retrieved from secret agent.
      --gcp-key-path string            Path to file containing service account JSON key.
      --gcp-bucket-name string         Name of the Google cloud storage bucket.
      --gcp-endpoint-override string   An alternate url endpoint to send GCP API calls to.

Azure Flags:
For Azure storage container name is mandatory, and is set with --azure-storage-container-name flag.
So --directory path will only contain folder name.
Flag --azure-endpoint is optional, and is used for tests with Azurit or any other Azure emulator.
For authentication you can use --azure-account-name and --azure-account-key, or 
--azure-tenant-id, --azure-client-id and azure-client-secret.
Any Azure parameter can be retrieved from secret agent.
      --azure-account-name string     Azure account name for account name, key authorization.
      --azure-account-key string      Azure account key for account name, key authorization.
      --azure-tenant-id string        Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string        Azure client ID for Azure Active Directory authorization.
      --azure-client-secret string    Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string         Azure endpoint.
      --azure-container-name string   Azure container Name.
```

## Unsupported flags
```

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
                           
--s3-min-part-size int
```
