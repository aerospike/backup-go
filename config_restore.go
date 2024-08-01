package backup

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// RestoreConfig contains configuration for the restore operation.
type RestoreConfig struct {
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
}

// NewDefaultRestoreConfig returns a new RestoreConfig with default values.
func NewDefaultRestoreConfig() *RestoreConfig {
	return &RestoreConfig{
		Parallel:        4,
		BatchSize:       128,
		MaxAsyncBatches: 16,
		EncoderType:     EncoderTypeASB,
	}
}

func (c *RestoreConfig) validate() error {
	if c.Parallel < MinParallel || c.Parallel > MaxParallel {
		return fmt.Errorf("parallel must be between 1 and 1024, got %d", c.Parallel)
	}

	if c.Namespace != nil {
		if err := c.Namespace.Validate(); err != nil {
			return fmt.Errorf("invalid restore namespace: %w", err)
		}
	}

	if c.Bandwidth < 0 {
		return fmt.Errorf("bandwidth value must not be negative, got %d", c.Bandwidth)
	}

	if c.RecordsPerSecond < 0 {
		return fmt.Errorf("rps value must not be negative, got %d", c.RecordsPerSecond)
	}

	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", c.BatchSize)
	}

	if c.MaxAsyncBatches <= 0 {
		return fmt.Errorf("max async batches must be positive, got %d", c.MaxAsyncBatches)
	}

	if err := c.CompressionPolicy.Validate(); err != nil {
		return fmt.Errorf("compression policy invalid: %w", err)
	}

	if err := c.EncryptionPolicy.Validate(); err != nil {
		return fmt.Errorf("encryption policy invalid: %w", err)
	}

	if err := c.SecretAgentConfig.Validate(); err != nil {
		return fmt.Errorf("secret agent invalid: %w", err)
	}

	return nil
}
