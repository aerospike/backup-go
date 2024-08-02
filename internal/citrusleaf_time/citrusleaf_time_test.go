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

package cltime

import (
	"reflect"
	"testing"
	"time"
)

func TestNow(t *testing.T) {
	now := time.Now()
	// Set the getNow function to return a static time for testing
	getNow = func() time.Time {
		return now
	}

	tests := []struct {
		name string
		want CLTime
	}{
		{
			name: "Test positive Now",
			want: CLTime{Seconds: now.Unix() - CitrusleafEpoch},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Now(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Now() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCLTime_Unix(t *testing.T) {
	type fields struct {
		Seconds int64
	}
	tests := []struct {
		name   string
		fields fields
		want   int64
	}{
		{
			name:   "Test positive Unix",
			fields: fields{Seconds: 0},
			want:   CitrusleafEpoch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CLTime{
				Seconds: tt.fields.Seconds,
			}
			if got := c.Unix(); got != tt.want {
				t.Errorf("CLTime.Unix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewCLTime(t *testing.T) {
	type args struct {
		seconds int64
	}
	tests := []struct {
		name string
		args args
		want CLTime
	}{
		{
			name: "Test positive NewCLTime",
			args: args{seconds: 100},
			want: CLTime{Seconds: 100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewCLTime(tt.args.seconds); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCLTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
