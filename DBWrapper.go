package backuplib

import (
	"errors"

	a "github.com/aerospike/aerospike-client-go/v7"
)

// WrappedAerospikeClient is a wrapper around the aerospike client
// that implements the WrappedAerospikeClient interface
type WrappedAerospikeClient struct {
	*a.Client
}

func NewWrappedAerospikeClient(ac *a.Client) (*WrappedAerospikeClient, error) {
	if ac == nil {
		return nil, errors.New("aerospike client pointer is nil")
	}

	return &WrappedAerospikeClient{
		Client: ac,
	}, nil
}

func (bc *WrappedAerospikeClient) RequestInfo(infoPolicy *a.InfoPolicy, names ...string) (map[string]string, error) {
	node := bc.Client.GetNodes()[0]

	return node.RequestInfo(infoPolicy, names...)
}
