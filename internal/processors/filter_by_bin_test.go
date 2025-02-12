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

package processors_test

import (
	"sync/atomic"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestFilterBin_NonRecordToken(t *testing.T) {
	skipped := &atomic.Uint64{}
	binList := []string{"bin1", "bin2"}
	processor := processors.NewFilterByBin(binList, skipped)

	token := &models.Token{
		Type: models.TokenTypeUDF,
	}

	result, err := processor.Process(token)

	assert.NoError(t, err)
	assert.Equal(t, token, result)
	assert.Equal(t, uint64(0), skipped.Load())
}

func TestFilterBin_RecordWithNoBins(t *testing.T) {
	skipped := &atomic.Uint64{}
	binList := []string{"bin1", "bin2"}
	processor := processors.NewFilterByBin(binList, skipped)

	token := &models.Token{
		Type: models.TokenTypeRecord,
		Record: &models.Record{
			Record: &a.Record{
				Bins: a.BinMap{},
			},
		},
	}

	result, err := processor.Process(token)

	assert.ErrorIs(t, err, pipeline.ErrFilteredOut)
	assert.Nil(t, result)
	assert.Equal(t, uint64(1), skipped.Load())
}

func TestFilterBin_RecordWithBinsToKeep(t *testing.T) {
	skipped := &atomic.Uint64{}
	binList := []string{"bin1", "bin2"}
	processor := processors.NewFilterByBin(binList, skipped)

	token := &models.Token{
		Type: models.TokenTypeRecord,
		Record: &models.Record{
			Record: &a.Record{
				Bins: a.BinMap{
					"bin1": "value1",
					"bin3": "value3",
				},
			},
		},
	}

	expectedBins := a.BinMap{
		"bin1": "value1",
	}

	result, err := processor.Process(token)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedBins, result.Record.Record.Bins)
	assert.Equal(t, uint64(0), skipped.Load())
}

func TestFilterBin_RecordWithAllBinsRemoved(t *testing.T) {
	skipped := &atomic.Uint64{}
	binList := []string{"bin1", "bin2"}
	processor := processors.NewFilterByBin(binList, skipped)

	token := &models.Token{
		Type: models.TokenTypeRecord,
		Record: &models.Record{
			Record: &a.Record{
				Bins: a.BinMap{
					"bin3": "value3",
				},
			},
		},
	}

	result, err := processor.Process(token)

	assert.ErrorIs(t, err, pipeline.ErrFilteredOut)
	assert.Nil(t, result)
	assert.Equal(t, uint64(1), skipped.Load())
}
