package processors

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

// NewChangeNamespace creates new changeNamespace
func NewChangeNamespace(namespace *models.RestoreNamespace) TokenProcessor {
	if namespace == nil {
		return &noopProcessor[*models.Token]{}
	}

	return &changeNamespace{
		namespace,
	}
}

// changeNamespace is used to restore to another namespace.
type changeNamespace struct {
	restoreNamespace *models.RestoreNamespace
}

// Process filters tokens by type.
func (p changeNamespace) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	key := token.Record.Key
	if key.Namespace() != *p.restoreNamespace.Source {
		return nil, fmt.Errorf("invalid namespace %s (expected: %s)", key.Namespace(), *p.restoreNamespace.Source)
	}

	newKey, err := a.NewKeyWithDigest(*p.restoreNamespace.Destination, key.SetName(), key.Value(), key.Digest())
	if err != nil {
		return nil, err
	}

	token.Record.Key = newKey

	return token, nil
}
