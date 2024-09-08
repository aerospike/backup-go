package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type GcpStorage struct {
	models.GcpStorage
}

func NewGcpStorage() *GcpStorage {
	return &GcpStorage{}
}

func (f *GcpStorage) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Host, "gcp-template",
		"",
		"The something")

	return flagSet
}

func (f *GcpStorage) GetGcpStorage() *models.GcpStorage {
	return &f.GcpStorage
}
