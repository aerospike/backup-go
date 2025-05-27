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

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
)

// changeNamespace is used to restore to another namespace.
type changeNamespace[T models.TokenConstraint] struct {
	source      *string
	destination *string
}

// NewChangeNamespace creates new changeNamespace
func NewChangeNamespace[T models.TokenConstraint](source, destination *string) processor[T] {
	if source == nil || destination == nil {
		return &noopProcessor[T]{}
	}

	return &changeNamespace[T]{
		source:      source,
		destination: destination,
	}
}

// Process filters tokens by type.
func (p changeNamespace[T]) Process(token T) (T, error) {
	t, ok := any(token).(*models.Token)
	if !ok {
		return nil, fmt.Errorf("unsupported token type %T for change namespace", token)
	}
	// if the token is not a record, we don't need to process it
	if t.Type != models.TokenTypeRecord {
		return token, nil
	}

	key := t.Record.Key
	if key.Namespace() != *p.source {
		return nil, fmt.Errorf("invalid namespace %s (expected: %s)", key.Namespace(), *p.source)
	}

	newKey, err := a.NewKeyWithDigest(*p.destination, key.SetName(), key.Value(), key.Digest())
	if err != nil {
		return nil, err
	}

	t.Record.Key = newKey

	return any(t).(T), nil
}
