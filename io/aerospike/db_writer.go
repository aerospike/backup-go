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

package aerospike

import "github.com/aerospike/aerospike-client-go/v8"

// dbWriter is an interface for writing data to an Aerospike cluster.
// The Aerospike Go client satisfies this interface.
//
//go:generate mockery --name dbWriter
type dbWriter interface {
	Put(policy *aerospike.WritePolicy, key *aerospike.Key, bins aerospike.BinMap) aerospike.Error

	CreateComplexIndex(
		policy *aerospike.WritePolicy,
		namespace,
		set,
		indexName,
		binName string,
		indexType aerospike.IndexType,
		indexCollectionType aerospike.IndexCollectionType,
		ctx ...*aerospike.CDTContext,
	) (*aerospike.IndexTask, aerospike.Error)

	DropIndex(policy *aerospike.WritePolicy, namespace, set, indexName string) aerospike.Error

	RegisterUDF(
		policy *aerospike.WritePolicy,
		udfBody []byte, serverPath string,
		language aerospike.Language,
	) (*aerospike.RegisterTask, aerospike.Error)

	BatchOperate(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error

	PutPayload(policy *aerospike.WritePolicy, key *aerospike.Key, payload []byte) aerospike.Error
}
