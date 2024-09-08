package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type AzureBlob struct {
	models.AzureBlob
}

func NewAzureBlob() *AzureBlob {
	return &AzureBlob{}
}

func (f *AzureBlob) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.Host, "azure-template",
		"",
		"The something")

	return flagSet
}

func (f *AzureBlob) GetAzureBlob() *models.AzureBlob {
	return &f.AzureBlob
}
