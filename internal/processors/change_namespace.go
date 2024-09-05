// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package processors

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/models"
)

// ChangeNamespace is used to restore to another namespace.
type ChangeNamespace struct {
	source      *string
	destination *string
}

// NewChangeNamespace creates new changeNamespace
func NewChangeNamespace(source, destination *string) TokenProcessor {
	if source == nil || destination == nil {
		return &noopProcessor[*models.Token]{}
	}

	return &ChangeNamespace{
		source:      source,
		destination: destination,
	}
}

// Process filters tokens by type.
func (p ChangeNamespace) Process(token *models.Token) (*models.Token, error) {
	// if the token is not a record, we don't need to process it
	if token.Type != models.TokenTypeRecord {
		return token, nil
	}

	key := token.Record.Key
	if key.Namespace() != *p.source {
		return nil, fmt.Errorf("invalid namespace %s (expected: %s)", key.Namespace(), *p.source)
	}

	newKey, err := a.NewKeyWithDigest(*p.destination, key.SetName(), key.Value(), key.Digest())
	if err != nil {
		return nil, err
	}

	token.Record.Key = newKey

	return token, nil
}
