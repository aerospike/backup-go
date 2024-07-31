package aerospike

import (
	"fmt"

	a "github.com/aerospike/aerospike-client-go/v7"
	atypes "github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/aerospike/backup-go/models"
)

type singleRecordWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	stats       *models.RestoreStats
}

func (rw *singleRecordWriter) writeRecord(record *models.Record) error {
	writePolicy := rw.writePolicy
	if rw.writePolicy.GenerationPolicy == a.EXPECT_GEN_GT {
		setGenerationPolicy := *rw.writePolicy
		setGenerationPolicy.Generation = record.Generation
		writePolicy = &setGenerationPolicy
	}

	aerr := rw.asc.Put(writePolicy, record.Key, record.Bins)
	if aerr != nil {
		if aerr.Matches(atypes.GENERATION_ERROR) {
			rw.stats.IncrRecordsFresher()
			return nil
		}

		if aerr.Matches(atypes.KEY_EXISTS_ERROR) {
			rw.stats.IncrRecordsExisted()
			return nil
		}

		return fmt.Errorf("error writing record %s: %w", record.Key.Digest(), aerr)
	}

	rw.stats.IncrRecordsInserted()

	return nil
}

func (rw *singleRecordWriter) close() error {
	return nil
}
