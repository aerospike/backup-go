package flags

import (
	"github.com/aerospike/backup-go/cmd/asbackup/models"
	"github.com/spf13/pflag"
)

type SecretAgent struct {
	models.SecretAgent
}

func NewSecretAgent() *SecretAgent {
	return &SecretAgent{}
}

func (f *SecretAgent) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.StringVar(&f.ConnectionType, "sa-connection-type",
		"tcp",
		"Secret agent connection type, supported types: tcp, unix.")
	flagSet.StringVar(&f.Address, "sa-address",
		"",
		"Secret agent host for TCP connection or socket file path for UDS connection.")
	flagSet.IntVar(&f.Port, "sa-port",
		0,
		"Secret agent port (only for TCP connection).")
	flagSet.IntVar(&f.TimeoutMillisecond, "sa-timeout",
		0,
		"Secret agent connection and reading timeout.")
	flagSet.StringVar(&f.CaFile, "sa-cafile",
		"",
		"Path to ca file for encrypted connection.")
	flagSet.BoolVar(&f.IsBase64, "sa-is-base64",
		false,
		"Flag that shows if secret agent responses are encrypted with base64.")

	return flagSet
}

func (f *SecretAgent) GetSecretAgent() *models.SecretAgent {
	return &f.SecretAgent
}
