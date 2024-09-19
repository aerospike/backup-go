# ASBackup
Aerospike Backup CLI tool.

## Build
### Dev
```bash
make build
```
### Release
```bash
# for local build
make release-test
# for release
make release
```

### Release requirements
For creating releases, you must have `goreleaser` installed on your device.
```bash
brew install goreleaser/tap/goreleaser
```
Configure `GITHUB_TOKEN`, `GITLAB_TOKEN` and `GITEA_TOKEN`.
More info here https://goreleaser.com/quick-start/

## Supported flags
```
Usage:
  asbackup [flags]

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

Backup Flags:
  -d, --directory string         The Directory that holds the backup files. Required, unless -o or -e is used.
  -n, --namespace string         The namespace to be backed up. Required.
  -s, --set stringArray          The set(s) to be backed up.
                                 If multiple sets are being backed up, filter-exp cannot be used.
                                 if empty all sets.
  -L, --records-per-second int   Limit total returned records per second (rps).
                                 Do not apply rps limit if records-per-second is zero.
  -B, --bin-list stringArray     Only include the given bins in the backup.
                                 If empty include all bins.
  -w, --parallel int             Maximum number of scan calls to run in parallel.
                                 If only one partition range is given, or the entire namespace is being backed up, the range
                                 of partitions will be evenly divided by this number to be processed in parallel. Otherwise, each
                                 filter cannot be parallelized individually, so you may only achieve as much parallelism as there are
                                 partition filters. (default 1)
  -R, --no-records               Don't backup any records.
  -I, --no-indexes               Don't backup any indexes.
      --no-udfs                  Don't backup any UDFs.
      --max-retries int          Maximum number of retries before aborting the current transaction. (default 5)
      --total-timeout int        Total socket timeout in milliseconds. 0 - no timeout.
      --socket-timeout int       Socket timeout in milliseconds. If this value is 0, its set to total-timeout. If both are 0,
                                 there is no socket idle time limit (default 10000)
  -N, --nice int                 The limits for read/write storage bandwidth in MiB/s
  -o, --output-file string          Backup to a single backup file. Use - for stdout. Required, unless -d or -e is used.
  -r, --remove-files                Remove existing backup file (-o) or files (-d).
  -F, --file-limit int              Rotate backup files, when their size crosses the given
                                    value (in bytes) Only used when backing up to a Directory.
  -D, --after-digest string         Backup records after record digest in record's partition plus all succeeding
                                    partitions. Used to resume backup with last record received from previous
                                    incomplete backup.
                                    This argument is mutually exclusive to partition-list.
                                    Format: base64 encoded string
                                    Example: EjRWeJq83vEjRRI0VniavN7xI0U=
                                    
  -a, --modified-before string      <YYYY-MM-DD_HH:MM:SS>
                                    Perform an incremental backup; only include records 
                                    that changed after the given date and time. The system's 
                                    local timezone applies. If only HH:MM:SS is specified, then
                                    today's date is assumed as the date. If only YYYY-MM-DD is 
                                    specified, then 00:00:00 (midnight) is assumed as the time.
                                    
  -b, --modified-after string       <YYYY-MM-DD_HH:MM:SS>
                                    Only include records that last changed before the given
                                    date and time. May combined with --modified-after to specify a range.
  -M, --max-records int             The number of records approximately to back up. 0 - all records
  -x, --no-bins                     Do not include bin data in the backup.
      --sleep-between-retries int   The amount of milliseconds to sleep between retries. (default 5)
  -f, --filter-exp string           Base64 encoded expression. Use the encoded filter expression in each scan call,
                                    which can be used to do a partial backup. The expression to be used can be base64 
                                    encoded through any client. This argument is mutually exclusive with multi-set backup.
                                    
      --parallel-nodes              Specifies how to perform scan. If set to true, we launch parallel workers for nodes;
                                    otherwise workers run in parallel for partitions.
      --remove-artifacts            Remove existing backup file (-o) or files (-d) without performing a backup.

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
      --s3-profile string             The S3 profile to use for credentials.
      --s3-endpoint-override string   An alternate url endpoint to send S3 API calls to.

GCP Flags:
      --gcp-key-path string            Path to file containing service account JSON key.
      --gcp-bucket-name string         Name of the Google cloud storage bucket.
      --gcp-endpoint-override string   An alternate url endpoint to send GCP API calls to.

Azure Flags:
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
--continue          Resumes an interrupted/failed backup from where it was left off, given the .state file
                    that was generated from the interrupted/failed run.

--state-file-dst    Either a path with a file name or a directory in which the backup state file will be
                    placed if the backup is interrupted/fails. If a path with a file name is used, that
                    exact path is where the backup file will be placed. If a directory is given, the backup
                    state will be placed in the directory with name `<namespace>.asb.state`, or
                    `<prefix>.asb.state` if `--output-file-prefix` is given.

--compact           Do not apply base-64 encoding to BLOBs; results in smaller backup files.
                    
--node-list         <IP addr 1>:<port 1>[,<IP addr 2>:<port 2>[,...]]
                    <IP addr 1>:<TLS_NAME 1>:<port 1>[,<IP addr 2>:<TLS_NAME 2>:<port 2>[,...]]
                    Backup the given cluster nodes only.
                    The job is parallelized over 16 scans unless --parallel is set to another value.
                    This argument is mutually exclusive to partition-list/after-digest arguments.
                    Default: backup all nodes in the cluster

--partition-list    <filter[,<filter>[...]]>
                    List of partitions to back up. Partition filters can be ranges, individual partitions, or 
                    records after a specific digest within a single partition.
                    This argument is mutually exclusive to after-digest.
                    Note: each partition filter is an individual task which cannot be parallelized, so you can only
                    achieve as much parallelism as there are partition filters. You may increase parallelism by dividing up
                    partition ranges manually.
                    Filter: <begin partition>[-<partition count>]|<digest>
                    begin partition: 0-4095
                    partition count: 1-4096 Default: 1
                    digest: base64 encoded string
                    Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=
                    Default: 0-4096 (all partitions)

--machine           Output machine-readable status updates to the given path, typically a FIFO.

--estimate          Estimate the backed-up record size from a random sample of 
                    10,000 (default) records at 99.9999%% confidence.

--estimate-samples  The number of samples to take when running a backup estimate.

--no-ttl-only       Only include records that have no ttl set (persistent records).

--prefer-racks      <rack id 1>[,<rack id 2>[,...]]
                    A list of Aerospike Server rack IDs to prefer when reading records for a backup.

--no-config-file    Do not read any config file. Default: disabled

--instance          Section with these instance is read. e.g in case instance `a` is specified
                    sections cluster_a, asbackup_a is read.
 
--config-file       Read this file after default configuration file.
  
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
                            
--s3-min-part-size int
```
