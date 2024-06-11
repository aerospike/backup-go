package aerospike

import (
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type recordBatchWriter struct {
	asc             dbWriter
	writePolicy     *a.WritePolicy
	stats           *models.RestoreStats
	logger          *slog.Logger
	operationBuffer []a.BatchRecordIfc
	batchSize       int
}

func (rw *recordBatchWriter) writeRecord(record *models.Record) error {
	writeOp := rw.batchWrite(record)
	rw.operationBuffer = append(rw.operationBuffer, writeOp)

	if len(rw.operationBuffer) > rw.batchSize {
		return rw.flushBuffer()
	}

	return nil
}

// batchWrite creates and returns a batch write operation for the record.
func (rw *recordBatchWriter) batchWrite(record *models.Record) *a.BatchWrite {
	policy := batchWritePolicy(rw.writePolicy, record)
	operations := putBinsOperations(record.Bins)

	return a.NewBatchWrite(policy, record.Key, operations...)
}

func batchWritePolicy(writePolicy *a.WritePolicy, r *models.Record) *a.BatchWritePolicy {
	policy := a.NewBatchWritePolicy()
	policy.RecordExistsAction = writePolicy.RecordExistsAction

	if writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		policy.GenerationPolicy = a.EXPECT_GEN_GT
		policy.Generation = r.Generation
	}

	return policy
}

func putBinsOperations(bins a.BinMap) []*a.Operation {
	ops := make([]*a.Operation, 0, len(bins))
	for k, v := range bins {
		ops = append(ops, a.PutOp(a.NewBin(k, v)))
	}

	return ops
}

func (rw *recordBatchWriter) close() error {
	return rw.flushBuffer()
}

func (rw *recordBatchWriter) flushBuffer() error {
	err := rw.asc.BatchOperate(nil, rw.operationBuffer)
	if err != nil {
		if !err.Matches(atypes.GENERATION_ERROR, atypes.KEY_EXISTS_ERROR) {
			return err
		}
	}

	for i := 0; i < len(rw.operationBuffer); i++ {
		switch rw.operationBuffer[i].BatchRec().ResultCode {
		case atypes.OK:
			rw.stats.IncrRecordsInserted()
		case atypes.GENERATION_ERROR:
			rw.stats.IncrRecordsFresher()
		case atypes.KEY_EXISTS_ERROR:
			rw.stats.IncrRecordsExisted()
		}
	}

	rw.operationBuffer = nil

	return nil
}
