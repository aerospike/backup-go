package processors

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

// NewChangeNamespaceProcessor creates new changeNamespaceProcessor
func NewChangeNamespaceProcessor(namespace *models.RestoreNamespace) TokenProcessor {
	if namespace == nil {
		return &noopProcessor[*models.Token]{}
	}

	return &changeNamespaceProcessor{
		namespace,
	}
}

// changeNamespaceProcessor is used to restore to another namespace.
type changeNamespaceProcessor struct {
	restoreNamespace *models.RestoreNamespace
}

// Process filters tokens by type.
func (p changeNamespaceProcessor) Process(token *models.Token) (*models.Token, error) {
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
