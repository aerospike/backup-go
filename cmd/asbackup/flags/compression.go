package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type Compression struct {
	models.Compression
}

func NewCompression() *Compression {
	return &Compression{}
}

func (f *Compression) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}
	flagSet.StringVarP(&f.Mode, "compress", "z",
		"NONE",
		"Enables compressing of backup files using the specified compression algorithm.\n"+
			"Supported compression algorithms are: ZSTD, NONE\n"+
			"Set the zstd compression level via the --compression-level option. Default level is 3.")
	flagSet.IntVar(&f.Level, "compression-level",
		3,
		"zstd compression level.")

	return flagSet
}

func (f *Compression) GetCompression() *models.Compression {
	return &f.Compression
}
