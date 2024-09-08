package models

type Backup struct {
	// Additional.
	Verbose bool
	// Map to backup.BackupConfig.ModBefore and ModAfter
	ModifiedBefore string
	ModifiedAfter  string

	Namespace string
	SetList   []string
	BinList   []string

	//	Partitions PartitionRange

	Parallel         int
	NoRecords        bool
	NoIndexes        bool
	NoUDFs           bool
	RecordsPerSecond int
	// 	Bandwidth int
	FileLimit   int64
	AfterDigest string
	// Scan policy params.
	MaxRetries          int
	MaxRecords          int64
	NoBins              bool
	SleepBetweenRetries int
	FilterExpression    string
	TotalTimeout        int
	SocketTimeout       int
}
