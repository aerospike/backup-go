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

package backup

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v7"
	cltime "github.com/aerospike/backup-go/encoding/citrusleaf_time"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/suite"
)

type proccessorTestSuite struct {
	suite.Suite
}

func (suite *proccessorTestSuite) TestProcessorWorker() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := newProcessorWorker(mockProcessor)
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

	worker := newProcessorWorker(mockProcessor)
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

	worker := newProcessorWorker(mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string, 1)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errors := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errors <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errors
	suite.NotNil(err)

	data := <-sender
	suite.Equal("test", data)
}

func (suite *proccessorTestSuite) TestProcessorWorkerCancelOnSend() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := newProcessorWorker(mockProcessor)
	suite.NotNil(worker)

	receiver := make(chan string, 1)
	receiver <- "test"

	worker.SetReceiveChan(receiver)

	sender := make(chan string)
	worker.SetSendChan(sender)

	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	errors := make(chan error, 1)
	go func() {
		defer wg.Done()
		err := worker.Run(ctx)
		errors <- err
	}()

	// give the worker some time to start
	time.Sleep(100 * time.Millisecond)

	cancel()
	wg.Wait()

	err := <-errors
	suite.NotNil(err)
}

func (suite *proccessorTestSuite) TestProcessorWorkerReceiveClosed() {
	mockProcessor := mocks.NewDataProcessor[string](suite.T())
	mockProcessor.EXPECT().Process("test").Return("test", nil)

	worker := newProcessorWorker(mockProcessor)
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

	worker := newProcessorWorker(mockProcessor)
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

func TestProcessorTTL_Process(t *testing.T) {
	mockStatsSetter := newMockStatsSetterExpired(t)
	mockStatsSetter.EXPECT().addRecordsExpired(uint64(1))

	mockStatsSetterNoExpectedCalls := newMockStatsSetterExpired(t)

	key, aerr := a.NewKey("test", "test", "test")
	if aerr != nil {
		t.Fatal(aerr)
	}

	type fields struct {
		getNow func() cltime.CLTime
		stats  statsSetterExpired
	}
	type args struct {
		token *models.Token
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Token
		wantErr bool
	}{
		{
			name: "Test positive Process expired",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 100}
				},
				stats: mockStatsSetter,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test positive Process expired v2",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 200}
				},
				stats: mockStatsSetter,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test positive token is not a record",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 200}
				},
				stats: mockStatsSetterNoExpectedCalls,
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
		{
			name: "Test positive Process",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
				stats: mockStatsSetterNoExpectedCalls,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: 100,
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &a.Record{
						Expiration: 50,
						Key:        key,
					},
					VoidTime: 100,
				},
			},
			wantErr: false,
		},
		{
			name: "Test positive Process never expire",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 50}
				},
				stats: mockStatsSetterNoExpectedCalls,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: models.VoidTimeNeverExpire,
					},
				},
			},
			want: &models.Token{
				Type: models.TokenTypeRecord,
				Record: models.Record{
					Record: &a.Record{
						Expiration: models.ExpirationNever,
						Key:        key,
					},
					VoidTime: models.VoidTimeNeverExpire,
				},
			},
			wantErr: false,
		},
		{
			name: "Test negative time difference too large",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 1}
				},
				stats: mockStatsSetterNoExpectedCalls,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: math.MaxInt64,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Test negative time difference too large",
			fields: fields{
				getNow: func() cltime.CLTime {
					return cltime.CLTime{Seconds: 1}
				},
				stats: mockStatsSetterNoExpectedCalls,
			},
			args: args{
				token: &models.Token{
					Type: models.TokenTypeRecord,
					Record: models.Record{
						Record: &a.Record{
							Key: key,
						},
						VoidTime: math.MaxInt64,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &processorTTL{
				getNow: tt.fields.getNow,
				stats:  tt.fields.stats,
				logger: slog.Default(),
			}
			got, err := p.Process(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("ProcessorTTL.Process() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ProcessorTTL.Process() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processorVoidTime_Process(t *testing.T) {
	type fields struct {
		getNow func() cltime.CLTime
	}
	type args struct {
		token *models.Token
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *models.Token
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
