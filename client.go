package backuplib

import (
	"errors"
	"io"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupMarshaller interface {
	MarshalRecord(*a.Record) ([]byte, error)
}

type Client struct {
	aerospikeClient *a.Client
	config          Config
}

func NewClient(ac *a.Client, cc Config) (*Client, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	return &Client{
		aerospikeClient: ac,
		config:          cc,
	}, nil
}

func (c *Client) getUsablePolicy(p *Policies) *Policies {
	policies := p
	if policies == nil {
		policies = c.config.Policies
	}
	if policies == nil {
		policies = &Policies{}
	}

	if policies.InfoPolicy == nil {
		policies.InfoPolicy = c.aerospikeClient.DefaultInfoPolicy
	}

	if policies.WritePolicy == nil {
		policies.WritePolicy = c.aerospikeClient.DefaultWritePolicy
	}

	if policies.ScanPolicy == nil {
		policies.ScanPolicy = c.aerospikeClient.DefaultScanPolicy
	}

	return policies
}

func (c *Client) BackupToWriter(writers []io.Writer, config *BackupToWriterConfig) (*BackupToWriterHandler, error) {
	if config == nil {
		config = NewBackupToWriterConfig()
	}
	config.Policies = c.getUsablePolicy(config.Policies)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newBackupToWriterHandler(config, c.aerospikeClient, writers)
	handler.run(writers)

	return handler, nil
}

func (c *Client) RestoreFromReader(readers []io.Reader, config *RestoreFromReaderConfig) (*RestoreFromReaderHandler, error) {
	if config == nil {
		config = NewRestoreFromReaderConfig()
	}
	config.Policies = c.getUsablePolicy(config.Policies)

	if err := config.validate(); err != nil {
		return nil, err
	}

	handler := newRestoreFromReaderHandler(config, c.aerospikeClient, readers)
	handler.run(readers)

	return handler, nil
}
