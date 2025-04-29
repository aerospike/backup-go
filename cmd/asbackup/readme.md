# ASBackup
Aerospike Backup CLI tool.

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
  asbackup [flags]
  asbackup xdr [flags]

General Flags:
  -Z, --help      Display help information.
  -V, --version   Display version information.
  -v, --verbose   Enable more detailed logging.

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
  -N, --nice int                 The limits for read/write storage bandwidth in MiB/s
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
                                      This argument is mutually exclusive to partition-list.
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
  -l, --node-list string              <IP addr 1>:<port 1>[,<IP addr 2>:<port 2>[,...]]
                                      <IP addr 1>:<TLS_NAME 1>:<port 1>[,<IP addr 2>:<TLS_NAME 2>:<port 2>[,...]]
                                      Back up the given cluster nodes only.
                                      The job is parallelized by number of nodes unless --parallel is set less than nodes number.
                                      This argument is mutually exclusive with --partition-list and --after-digest arguments.
                                      Default: backup all nodes in the cluster
  -X, --partition-list string         List of partitions <filter[,<filter>[...]]> to back up. Partition filters can be ranges,
                                      individual partitions, or records after a specific digest within a single partition.
                                      This argument is mutually exclusive to after-digest.
                                      Filter: <begin partition>[-<partition count>]|<digest>
                                      begin partition: 0-4095
                                      partition count: 1-4096 Default: 1
                                      digest: Base64 encoded string
                                      Examples: 0-1000, 1000-1000, 2222, EjRWeJq83vEjRRI0VniavN7xI0U=
                                      Default: 0-4096 (all partitions)
                                      
      --prefer-racks string           <rack id 1>[,<rack id 2>[,...]]
                                      A list of Aerospike Database rack IDs to prefer when reading records for a backup.
      --rack-list string              <rack id 1>[,<rack id 2>[,...]]
                                      A list of Aerospike Database rack IDs to backup.
                                      Unlike --prefer-racks, only specified racks will be backed up.
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
  -c, --continue string               Resumes an interrupted/failed backup from where it was left off, given the .state file
                                      that was generated from the interrupted/failed run.
      --state-file-dst string         Name of a state file that will be saved in backup --directory.
                                      Works only with --file-limit parameter. As --file-limit is reached and the file is closed,
                                      the current state will be saved. Works only for default and/or partition backup.
                                      Not work with --parallel-nodes or --node--list.
      --scan-page-size int            Number of records will be read on one iteration for continuation backup.
                                      Affects size if overlap on resuming backup after an error.
                                      Used only with --state-file-dst or --continue. (default 10000)
      --info-retry-timeout int        Set the initial timeout for a retry in milliseconds when info commands are sent.This parameter is applied to stop xdr and unblock MRT writes requests. (default 1000)
      --info-retry-multiplier float   Increases the delay between subsequent retry attempts.
                                      The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber) (default 1)
      --info-max-retries uint         How many times to retry to send info commands before failing. This parameter is applied to stop xdr and unblock MRT writes requests. (default 3)

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
                                      the server in a single request. Objects smaller than the size will be sent in a single request,
                                      while larger objects will be split over multiple requests.  (default 5242880)

GCP Flags:
For GCP storage bucket name is mandatory, and is set with --gcp-bucket-name flag.
So --directory path will only contain folder name.
Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.
Any GCP parameter can be retrieved from Secret Agent.
      --gcp-key-path string            Path to file containing service account JSON key.
      --gcp-bucket-name string         Name of the Google cloud storage bucket.
      --gcp-endpoint-override string   An alternate url endpoint to send GCP API calls to.
      --gcp-chunk-size int             Chunk size controls the maximum number of bytes of the object that the app will attempt to send to
                                       the server in a single request. Objects smaller than the size will be sent in a single request,
                                       while larger objects will be split over multiple requests.  (default 5242880)

Azure Flags:
For Azure storage container name is mandatory, and is set with --azure-storage-container-name flag.
So --directory path will only contain folder name.
Flag --azure-endpoint is optional, and is used for tests with Azurit or any other Azure emulator.
For authentication you can use --azure-account-name and --azure-account-key, or 
--azure-tenant-id, --azure-client-id and azure-client-secret.
Any Azure parameter can be retrieved from Secret Agent.
      --azure-account-name string     Azure account name for account name, key authorization.
      --azure-account-key string      Azure account key for account name, key authorization.
      --azure-tenant-id string        Azure tenant ID for Azure Active Directory authorization.
      --azure-client-id string        Azure client ID for Azure Active Directory authorization.
      --azure-client-secret string    Azure client secret for Azure Active Directory authorization.
      --azure-endpoint string         Azure endpoint.
      --azure-container-name string   Azure container Name.
      --azure-access-tier string      Azure access tier is applied to created backup files.
                                      Tiers are: Archive, Cold, Cool, Hot, P10, P15, P20, P30, P4, P40, P50, P6, P60, P70, P80, Premium.
      --azure-block-size int          Block size defines the size of the buffer used during upload. (default 5242880)
```

## XDR backup. 
For XDR backup the client starts a TCP server and accepts connections from the database.

```
Usage:
  asbackup xdr [flags]

XDR Backup Flags:
This sections replace Backup Flags section in main documentation.
All other flags are valid for XDR backup.
  -n, --namespace string              The namespace to be backed up. Required.
  -d, --directory string              The directory that holds the backup files. Required.
  -r, --remove-files                  Remove an existing backup file (-o) or entire directory (-d) and replace with the new backup.
  -F, --file-limit int                Rotate backup files when their size crosses the given
                                      value (in bytes). Only used when backing up to a directory.
                                       (default 262144000)
      --parallel-write int            Number of concurrent backup files writing.
                                      If not set, the default value is automatically calculated and appears as the number of CPUs on your machine.
      --dc string                     DC that will be created on source instance for xdr backup.
                                      DC name can include only Latin lowercase and uppercase letters with no diacritical marks (a-z, A-Z),
                                      digits 0-9, underscores (_), hyphens (-), and dollar signs ($). Max length is 31 bytes. (default "dc")
      --local-address string          Local IP address that the XDR server listens on. (default "127.0.0.1")
      --local-port int                Local port that the XDR server listens on. (default 8080)
      --rewind string                 Rewind is used to ship all existing records of a namespace.
                                      When rewinding a namespace, XDR will scan through the index and ship
                                      all the records for that namespace, partition by partition.
                                      Can be the string "all" or an integer number of seconds. (default "all")
      --max-throughput int            Number of records per second to ship using XDR.The --max-throughput value should be in multiples of 100.
                                      If 0, the default server value will be used.
      --read-timeout int              Timeout in milliseconds for TCP read operations. Used by TCP server for XDR. (default 1000)
      --write-timeout int             Timeout in milliseconds for TCP write operations. Used by TCP server for XDR. (default 1000)
      --results-queue-size int        Buffer for processing messages received from XDR. (default 256)
      --ack-queue-size int            Buffer for processing acknowledge messages sent to XDR. (default 256)
      --max-connections int           Maximum number of concurrent TCP connections. (default 256)
      --info-poling-period int        How often (in milliseconds) a backup client sends info commands
                                      to check Aerospike cluster statistics on recovery rate and lag. (default 1000)
      --info-retry-timeout int        Set the initial timeout for a retry in milliseconds when info commands are sent.
                                      This parameter is applied to stop-xdr and unblock-mrt requests. (default 1000)
      --info-retry-multiplier float   Increases the delay between subsequent retry attempts.
                                      The actual delay is calculated as: info-retry-timeout * (info-retry-multiplier ^ attemptNumber) (default 1)
      --info-max-retries uint         How many times to retry sending info commands before failing.
                                       This parameter is applied to stop-xdr and unblock-mrt requests. (default 3)
      --start-timeout int             Timeout for starting TCP server for XDR.
                                      If the TCP server for XDR does not receive any data within this timeout period, it will shut down.
                                      This situation can occur if the --local-address and --local-port options are misconfigured. (default 30000)
      --stop-xdr                      Stops XDR and removes XDR configuration from the database.
                                      Used if previous XDR backup was interrupted or failed, but the database server still sends XDR events.
                                      Use this functionality to stop XDR after an interrupted backup.
      --unblock-mrt                   Unblock MRT writes on the database.
                                      Use this functionality to unblock MRT writes after an interrupted backup.
```


## Unsupported flags
```
--machine           Output machine-readable status updates to the given path, typically a FIFO.

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
```
