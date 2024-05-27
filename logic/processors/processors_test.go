// Copyright 2024-2024 Aerospike, Inc.
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
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/logic/processors/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type proccessorTestSuite struct {
	suite.Suite
}

func (suite *proccessorTestSuite) TestProcessorWorker() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *proccessorTestSuite) TestProcessorWorkerFilteredOut() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", errFilteredOut)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)

	suite.Equal(0, len(sender))
}

func (suite *proccessorTestSuite) TestProcessorWorkerCancelOnReceive() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errorCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errorCh <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errorCh
	suite.NotNil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *proccessorTestSuite) TestProcessorWorkerCancelOnSend() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errorCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errorCh <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errorCh
	suite.NotNil(err)
}

func (suite *proccessorTestSuite) TestProcessorWorkerReceiveClosed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.Nil(err)
}

func (suite *proccessorTestSuite) TestProcessorWorkerProcessFailed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("", errors.New("test"))

	worker := NewProcessorWorker[string](mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"
	close(receiver)

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx := context.Background()
	err := worker.Run(ctx)
	suite.NotNil(err)
}

func TestProcessors(t *testing.T) {
	suite.Run(t, new(proccessorTestSuite))
}

func Test_processorVoidTime_Process(t *testing.T) {
	type fields struct {
		getNow func() cltime.CLTime
	}
	type args struct {
		token *models.Token
	}
	tests := []struct {
		fields  fields
		args    args
		want    *models.Token
		name    string
		wantErr bool
	}{
		{
			name: "Test positive Process",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Expiration: 100,
						},
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &a.Record{
						Expiration: 100,
					},
					VoidTime: 150,
				},
			},
			wantErr: false,
		},
		{
			name: "Test positive never expire",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Expiration: models.ExpirationNever,
						},
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &a.Record{
						Expiration: models.ExpirationNever,
					},
					VoidTime: models.VoidTimeNeverExpire,
				},
			},
			wantErr: false,
		},
		{
			name: "Test positive token is not a record",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeSIndex,
				},
			},
			want: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &processorVoidTime{
				getNow: tt.fields.getNow,
			}
			got, err := p.Process(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("processorVoidTime.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processorVoidTime.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTPSLimiter(t *testing.T) {
	t.Skip()
	tests := []struct {
		name string
		tps  int
		runs int
	}{
		{name: "zero tps", tps: 0, runs: 1000},
		{name: "tps 20", tps: 20, runs: 50},
		{name: "tps 500", tps: 500, runs: 2_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewTPSLimiter[int](tt.tps)

			start := time.Now()
			for i := range tt.runs {
				got, err := limiter.Process(i)
				if got != i {
					t.Fatalf("Process() = %v, want %v", got, i)
				}
				if err != nil {
					t.Fatalf("got error while processing token: %v", err)
				}
			}
			duration := time.Since(start)

			const epsilon = 100 * time.Millisecond
			var expectedDuration time.Duration
			if tt.tps > 0 {
				timeRequiredSeconds := float64(tt.runs) / float64(tt.tps)
				expectedDuration = time.Duration(int(timeRequiredSeconds*1000)) * time.Millisecond
			}
			if duration < expectedDuration-epsilon {
				t.Fatalf("Total execution time was too quick, want at least %v, got %v", expectedDuration, duration)
			}
			if duration > expectedDuration+epsilon {
				t.Fatalf("Total execution time was too slow, want at most %v, got %v", expectedDuration, duration)
			}
		})
	}
}

func TestSetFilter(t *testing.T) {
	type test struct {
		name             string
		token            *models.Token
		setFilter        *setFilterProcessor
		shouldBeFiltered bool
	}

	setName := "set"
	key, _ := a.NewKey("", setName, "")
	record := models.Record{
		Record: &a.Record{
			Key: key,
		},
	}
	tests := []test{
		{
			name: "Non-record token type",
			token: &models.Token{
				Type: models.TokenTypeSIndex,
			},
			setFilter: &setFilterProcessor{
				setsToRestore: map[string]bool{
					"test": true,
				},
			},
			shouldBeFiltered: false,
		},
		{
			name: "No sets to restore",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter:        &setFilterProcessor{setsToRestore: map[string]bool{}},
			shouldBeFiltered: false,
		},
		{
			name: "Token set not in restore list",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter: &setFilterProcessor{
				setsToRestore: map[string]bool{
					"anotherSet": true,
				},
				skipped: &atomic.Uint64{},
			},
			shouldBeFiltered: true,
		},
		{
			name: "Token set in restore list",
			token: &models.Token{
				Type:   models.TokenTypeRecord,
				Record: record,
			},
			setFilter: &setFilterProcessor{
				setsToRestore: map[string]bool{
					setName: true,
				},
			},
			shouldBeFiltered: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resToken, resErr := tc.setFilter.Process(tc.token)
			if tc.shouldBeFiltered {
				assert.Nil(t, resToken)
				assert.NotNil(t, resErr)
			} else {
				assert.Equal(t, tc.token, resToken)
				assert.Nil(t, resErr)
			}
		})
	}
}

func TestChangeNamespaceProcessor(t *testing.T) {
	restoreNamespace := models.RestoreNamespace{
		Source:      ptr.String("sourceNS"),
		Destination: ptr.String("destinationNS"),
	}

	key, _ := a.NewKey(*restoreNamespace.Source, "set", 1)
	invalidKey, _ := a.NewKey("otherNs", "set", 1)

	tests := []struct {
		name         string
		restoreNS    *models.RestoreNamespace
		initialToken *models.Token
		wantErr      bool
	}{
		{
			name:      "nil restore Namespace",
			restoreNS: nil,
			initialToken: models.NewRecordToken(models.Record{
				Record: &a.Record{
					Key: key,
				},
			}, 0),
			wantErr: false,
		},
		{
			name:         "non-record Token Type",
			restoreNS:    &restoreNamespace,
			initialToken: models.NewUDFToken(nil, 0),
			wantErr:      false,
		},
		{
			name:      "invalid source namespace",
			restoreNS: &restoreNamespace,
			initialToken: models.NewRecordToken(models.Record{
				Record: &a.Record{
					Key: invalidKey,
				},
			}, 0),
			wantErr: true,
		},
		{
			name:      "valid process",
			restoreNS: &restoreNamespace,
			initialToken: models.NewRecordToken(models.Record{
				Record: &a.Record{
					Key: key,
				},
			}, 0),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewChangeNamespaceProcessor(tt.restoreNS)
			gotToken, err := p.Process(tt.initialToken)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.initialToken.Type == models.TokenTypeRecord && tt.restoreNS != nil {
					assert.Equal(t, *tt.restoreNS.Destination, gotToken.Record.Key.Namespace())
				}
			}
		})
	}
}
