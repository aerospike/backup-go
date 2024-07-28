package backup

import (
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

// BackupConfig contains configuration for the backup operation.
type BackupConfig struct {
	// EncoderType describes an encoder type that will be used on backing up.
	// Default `EncoderTypeASB` = 0.
	EncoderType EncoderType
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// ScanPolicy applies to Aerospike scan operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	ScanPolicy *a.ScanPolicy
	// Only include records that last changed before the given time (optional).
	ModBefore *time.Time
	// Only include records that last changed after the given time (optional).
	ModAfter *time.Time
	// Encryption details.
	EncryptionPolicy *models.EncryptionPolicy
	// Compression details.
	CompressionPolicy *models.CompressionPolicy
	// Secret agent config.
	SecretAgentConfig *models.SecretAgentConfig
	// Namespace is the Aerospike namespace to back up.
	Namespace string
	// SetList is the Aerospike set to back up (optional, given an empty list,
	// all sets will be backed up).
	SetList []string
	// The list of backup bin names (optional, given an empty list, all bins will be backed up)
	BinList []string
	// Partitions specifies the Aerospike partitions to back up.
	Partitions PartitionRange
	// Parallel is the number of concurrent scans to run against the Aerospike cluster.
	Parallel int
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
	// File size limit (in bytes) for the backup. If a backup file crosses this size threshold,
	// a new file will be created. 0 for no file size limit.
	FileLimit int64
}

// NewBackupConfig returns a new BackupConfig with default values.
func NewBackupConfig() *BackupConfig {
	return &BackupConfig{
		Partitions:  PartitionRange{0, MaxPartitions},
		Parallel:    1,
		Namespace:   "test",
		EncoderType: EncoderTypeASB,
	}
}

// RestoreConfig contains configuration for the restore operation.
type RestoreConfig struct {
	// EncoderType describes an encoder type that will be used on restoring.
	// Default `EncoderTypeASB` = 0.
	EncoderType EncoderType
	// InfoPolicy applies to Aerospike Info requests made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	InfoPolicy *a.InfoPolicy
	// WritePolicy applies to Aerospike write operations made during backup and restore
	// If nil, the Aerospike client's default policy will be used.
	WritePolicy *a.WritePolicy
	// Namespace details for the restore operation.
	// By default, the data is restored to the namespace from which it was taken.
	Namespace *models.RestoreNamespace `json:"namespace,omitempty"`
	// Encryption details.
	EncryptionPolicy *models.EncryptionPolicy
	// Compression details.
	CompressionPolicy *models.CompressionPolicy
	// Secret agent config.
	SecretAgent *models.SecretAgentConfig
	// The sets to restore (optional, given an empty list, all sets will be restored).
	SetList []string
	// The bins to restore (optional, given an empty list, all bins will be restored).
	BinList []string
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
}

// NewRestoreConfig returns a new RestoreConfig with default values.
func NewRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		Parallel:        4,
		BatchSize:       128,
		MaxAsyncBatches: 16,
		EncoderType:     EncoderTypeASB,
	}
}
