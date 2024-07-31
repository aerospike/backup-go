package aerospike

import "github.com/aerospike/aerospike-client-go/v7"

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
}
