package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type Storage struct {
	models.Storage
}

func NewStorage() *Storage {
	return &Storage{}
}

func (f *Storage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVarP(&f.Directory, "Directory", "d",
		"",
		"The Directory that holds the backup files. Required, unless -o or -e is used.")
	flagSet.StringVarP(&f.OutputFile, "output-file", "o",
		"",
		"Backup to a single backup file. Use - for stdout. Required, unless -d or -e is used.")
	flagSet.BoolVarP(&f.RemoveFiles, "remove-files", "r",
		false,
		"Remove existing backup file (-o) or files (-d).")

	return flagSet
}

func (f *Storage) GetStorage() *models.Storage {
	return &f.Storage
}
