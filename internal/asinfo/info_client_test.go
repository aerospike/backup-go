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

package asinfo

import (
	"encoding/base64"
	"errors"
	"reflect"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/asinfo/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testASDC            = "DC1"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testASRewind        = "all"
	testXDRHostPort     = "127.0.0.1:3003"
)

func Test_parseAerospikeVersion(t *testing.T) {
	type args struct {
		versionStr string
	}
	tests := []struct {
		name    string
		args    args
		want    AerospikeVersion
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				versionStr: "5.6.0.0",
			},
			want: AerospikeVersion{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
		},
		{
			name: "positive multiple digits",
			args: args{
				versionStr: "1829.123.0.33333",
			},
			want: AerospikeVersion{
				Major: 1829,
				Minor: 123,
				Patch: 0,
			},
		},
		{
			name: "positive development version",
			args: args{
				versionStr: "7.1.0.0-rc1-g1234",
			},
			want: AerospikeVersion{
				Major: 7,
				Minor: 1,
				Patch: 0,
			},
		},
		{
			name: "negative too few digits",
			args: args{
				versionStr: "5.6",
			},
			wantErr: true,
		},
		{
			name: "negative bad major version",
			args: args{
				versionStr: "A.6.0",
			},
			wantErr: true,
		},
		{
			name: "negative bad minor version",
			args: args{
				versionStr: "5.A.0",
			},
			wantErr: true,
		},
		{
			name: "negative bad patch version",
			args: args{
				versionStr: "5.6.A",
			},
			wantErr: true,
		},
		{
			name: "negative no match",
			args: args{
				versionStr: "asdfasdf",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAerospikeVersion(tt.args.versionStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseAerospikeVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseAerospikeVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAerospikeVersion_IsGreaterThan(t *testing.T) {
	type fields struct {
		Major int
		Minor int
		Patch int
	}
	type args struct {
		other AerospikeVersion
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "positive major",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 4,
					Minor: 1,
					Patch: 0,
				},
			},
			want: true,
		},
		{
			name: "positive minor",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 5,
					Patch: 0,
				},
			},
			want: true,
		},
		{
			name: "positive patch",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 1,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 0,
				},
			},
			want: true,
		},
		{
			name: "positive equal",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 0,
				},
			},
			want: false,
		},
		{
			name: "positive major less",
			fields: fields{
				Major: 4,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 0,
				},
			},
			want: false,
		},
		{
			name: "positive minor less",
			fields: fields{
				Major: 5,
				Minor: 5,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 0,
				},
			},
			want: false,
		},
		{
			name: "positive patch less",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 1,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			av := AerospikeVersion{
				Major: tt.fields.Major,
				Minor: tt.fields.Minor,
				Patch: tt.fields.Patch,
			}
			if got := av.IsGreater(tt.args.other); got != tt.want {
				t.Errorf("AerospikeVersion.IsGreaterThan() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildSindexCmd(t *testing.T) {
	type args struct {
		namespace string
		getCtx    bool
	}
	tests := []struct {
		name string
		want string
		args args
	}{
		{
			name: "positive no ctx",
			args: args{
				namespace: "test",
				getCtx:    false,
			},
			want: "sindex-list:ns=test",
		},
		{
			name: "positive with ctx",
			args: args{
				namespace: "test",
				getCtx:    true,
			},
			want: "sindex-list:ns=test;b64=true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildSindexCmd(tt.args.namespace, tt.args.getCtx); got != tt.want {
				t.Errorf("buildSindexCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseSIndex(t *testing.T) {
	cdtCtx, err := a.CDTContextToBase64([]*a.CDTContext{a.CtxListValue(a.NewValue([]byte("hi")))})
	if err != nil {
		panic(err)
	}

	type args struct {
		sindexMap infoMap
	}
	tests := []struct {
		args    args
		want    *models.SIndex
		name    string
		wantErr bool
	}{
		{
			name: "positive bin (default) sindex",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive bin (none) sindex",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "none",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive list elements sindex",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "list",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.ListElementSIndex,
			},
		},
		{
			name: "positive map keys sindex",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "mapkeys",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.MapKeySIndex,
			},
		},
		{
			name: "positive map values sindex",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "mapvalues",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.MapValueSIndex,
			},
		},
		{
			name: "positive ctx",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "mapvalues",
					"context":   cdtCtx,
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName:    "testbin",
					BinType:    models.NumericSIDataType,
					B64Context: cdtCtx,
				},
				IndexType: models.MapValueSIndex,
			},
		},
		{
			name: "positive null set",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "null",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive numeric bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "numeric",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive numeric (int signed) bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "int signed",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.NumericSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive string bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "string",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.StringSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive string (text) bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "text",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.StringSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive blob bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "blob",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.BlobSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive geojson (geo2dsphere) bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geo2dsphere",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.GEO2DSphereSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "positive geojson bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geojson",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				Path: models.SIndexPath{
					BinName: "testbin",
					BinType: models.GEO2DSphereSIDataType,
				},
				IndexType: models.BinSIndex,
			},
		},
		{
			name: "negative response missing namespace",
			args: args{
				sindexMap: infoMap{
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geojson",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing indexname",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geojson",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing indextype",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geojson",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing bin",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"type":      "geojson",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing type",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative invalid indextype",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "geojson",
					"indextype": "badtype",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative invalid type",
			args: args{
				sindexMap: infoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "badtype",
					"indextype": "default",
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSIndex(tt.args.sindexMap)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseInfoResponse(t *testing.T) {
	type args struct {
		resp    string
		objSep  string
		pairSep string
		kvSep   string
	}
	tests := []struct {
		name    string
		args    args
		want    []infoMap
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				resp:    "foo=bar",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: []infoMap{
				{
					"foo": "bar",
				},
			},
		},
		{
			name: "positive empty",
			args: args{
				resp:    "",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: nil,
		},
		{
			name: "positive delimiter only",
			args: args{
				resp:    ";",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: nil,
		},
		{
			name: "positive value includes '='",
			args: args{
				resp:    "foo=bar=as",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: []infoMap{
				{
					"foo": "bar=as",
				},
			},
		},
		{
			name: "positive multiple pairs",
			args: args{
				resp:    "foo=bar:baz=qux",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: []infoMap{
				{
					"foo": "bar",
					"baz": "qux",
				},
			},
		},
		{
			name: "positive multiple objects",
			args: args{
				resp:    "foo=bar:baz=qux;bar=foo:qux=bar",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: []infoMap{
				{
					"foo": "bar",
					"baz": "qux",
				},
				{
					"bar": "foo",
					"qux": "bar",
				},
			},
		},
		{
			name: "positive multiple objects",
			args: args{
				resp:    "foo=bar:baz=qux;bar=foo:qux=bar",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			want: []infoMap{
				{
					"foo": "bar",
					"baz": "qux",
				},
				{
					"bar": "foo",
					"qux": "bar",
				},
			},
		},
		{
			name: "positive other format objects",
			args: args{
				resp:    "foo.bar,baz.qux|bar.foo,qux.bar",
				objSep:  "|",
				pairSep: ",",
				kvSep:   ".",
			},
			want: []infoMap{
				{
					"foo": "bar",
					"baz": "qux",
				},
				{
					"bar": "foo",
					"qux": "bar",
				},
			},
		},
		{
			name: "negative invalid key pair",
			args: args{
				resp:    "foo=bar:bazqux;bar=foo:qux=bar",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			wantErr: true,
		},
		{
			name: "negative no syntax",
			args: args{
				resp:    "asdfasdfasdf",
				objSep:  ";",
				pairSep: ":",
				kvSep:   "=",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInfoResponse(tt.args.resp, tt.args.objSep, tt.args.pairSep, tt.args.kvSep)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInfoResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseInfoResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newMockInfoGetter(t *testing.T, arg string, resp map[string]string, err a.Error) infoGetter {
	t.Helper()
	mockInfoGetter := mocks.NewMockinfoGetter(t)
	mockInfoGetter.On("RequestInfo", (*a.InfoPolicy)(nil), arg).Return(resp, err)
	return mockInfoGetter
}

func Test_getAerospikeVersion(t *testing.T) {
	type args struct {
		node   infoGetter
		policy *a.InfoPolicy
	}
	tests := []struct {
		args    args
		name    string
		want    AerospikeVersion
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				node: newMockInfoGetter(t, "build", map[string]string{"build": "5.6.0.0"}, nil),
			},
			want: AerospikeVersion{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
		},
		{
			name: "positive dev build",
			args: args{
				node: newMockInfoGetter(t, "build", map[string]string{"build": "7.6.1.0-rc2-ghasd"}, nil),
			},
			want: AerospikeVersion{
				Major: 7,
				Minor: 6,
				Patch: 1,
			},
		},
		{
			name: "negative request info fails",
			args: args{
				node: newMockInfoGetter(t, "build", nil, a.ErrNetTimeout),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getAerospikeVersion(tt.args.node, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("getAerospikeVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getAerospikeVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSIndexes(t *testing.T) {
	mockInfoGetterNoCtx := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(
		map[string]string{"build": "5.6.0.0"},
		nil,
	)
	mockInfoGetterNoCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:ns=test").Return(map[string]string{
		"sindex-list:ns=test": "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW",
	}, nil)

	mockInfoGetterCtx := mocks.NewMockinfoGetter(t)
	mockInfoGetterCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(map[string]string{"build": "7.1.0.0"}, nil)
	mockInfoGetterCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:ns=test;b64=true").Return(map[string]string{
		"sindex-list:ns=test;b64=true": "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=AAAAAA==:state=RW",
	}, nil)

	mockInfoGetterGetBuildFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetBuildFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(nil, a.ErrNetTimeout)

	mockInfoGetterGetSIndexesFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(map[string]string{"build": "7.1.0.0"}, nil)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:ns=test;b64=true").Return(nil, a.ErrNetwork)

	type args struct {
		conn      infoGetter
		policy    *a.InfoPolicy
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		want    []*models.SIndex
		wantErr bool
	}{
		{
			name: "positive no ctx",
			args: args{
				conn:      mockInfoGetterNoCtx,
				namespace: "test",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "positive with ctx",
			args: args{
				conn:      mockInfoGetterCtx,
				namespace: "test",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName:    "testbin",
						BinType:    models.NumericSIDataType,
						B64Context: "AAAAAA==",
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "negative get build fails",
			args: args{
				conn:      mockInfoGetterGetBuildFailed,
				namespace: "test",
			},
			wantErr: true,
		},
		{
			name: "negative get sindex fails",
			args: args{
				conn:      mockInfoGetterGetSIndexesFailed,
				namespace: "test",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getSIndexes(tt.args.conn, tt.args.namespace, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSIndexes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSIndexes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseSIndexResponse(t *testing.T) {
	type args struct {
		sindexInfoResp string
	}
	tests := []struct {
		name    string
		args    args
		want    []*models.SIndex
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "positive multiple sindexes",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW;ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "positive with trailing semicolon",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW;",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "positive no sindexes",
			args: args{
				sindexInfoResp: "",
			},
			want: nil,
		},
		{
			name: "negative bad response",
			args: args{
				sindexInfoResp: "invalid info response",
			},
			wantErr: true,
		},
		{
			name: "negative bad sindex",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=testindex:bin=testbin:type=BADTYPE:indextype=default:context=null:state=RW",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSIndexes(tt.args.sindexInfoResp)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSIndexResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSIndexResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAerospikeVersion_IsGreaterOrEqual(t *testing.T) {
	type fields struct {
		Major int
		Minor int
		Patch int
	}
	type args struct {
		other AerospikeVersion
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "positive greater",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 4,
					Minor: 1,
					Patch: 0,
				},
			},
			want: true,
		},
		{
			name: "positive equal",
			fields: fields{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 5,
					Minor: 6,
					Patch: 0,
				},
			},
			want: true,
		},
		{
			name: "positive less",
			fields: fields{
				Major: 4,
				Minor: 0,
				Patch: 0,
			},
			args: args{
				other: AerospikeVersion{
					Major: 4,
					Minor: 1,
					Patch: 0,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			av := AerospikeVersion{
				Major: tt.fields.Major,
				Minor: tt.fields.Minor,
				Patch: tt.fields.Patch,
			}
			if got := av.IsGreaterOrEqual(tt.args.other); got != tt.want {
				t.Errorf("AerospikeVersion.IsGreaterOrEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseUDF(t *testing.T) {
	type args struct {
		udfMap infoMap
	}
	tests := []struct {
		args    args
		want    *models.UDF
		name    string
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				udfMap: infoMap{
					"type":    "LUA",
					"content": base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				},
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Content: []byte("function test()\n return 1\n end\n"),
			},
		},
		{
			name: "negative missing type",
			args: args{
				udfMap: infoMap{
					"content": base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				},
			},
			wantErr: true,
		},
		{
			name: "negative bad language type",
			args: args{
				udfMap: infoMap{
					"type":    "BADTYPE",
					"content": base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				},
			},
			wantErr: true,
		},
		{
			name: "negative missing content",
			args: args{
				udfMap: infoMap{
					"type": "LUA",
				},
			},
			wantErr: true,
		},
		{
			name: "negative content is not base64 encoded",
			args: args{
				udfMap: infoMap{
					"type":    "LUA",
					"content": "function test()\n return 1\n end\n",
				},
			},
			wantErr: true,
		},
		{
			name: "negative wrong number of info elements",
			args: args{
				udfMap: infoMap{
					"type":     "LUA",
					"content":  "function test()\n return 1\n end\n",
					"too many": "elements",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUDF(tt.args.udfMap)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUDF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseUDF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseUDFResponse(t *testing.T) {
	type args struct {
		udfInfoResp string
	}
	tests := []struct {
		want    *models.UDF
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				udfInfoResp: "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Content: []byte("function test()\n return 1\n end\n"),
			},
		},
		{
			name: "negative empty get udf response",
			args: args{
				udfInfoResp: "",
			},
			wantErr: true,
		},
		{
			name: "negative bad info response",
			args: args{
				udfInfoResp: "badresponse;;;;;;-----;",
			},
			wantErr: true,
		},
		{
			name: "negative bad udf info response",
			args: args{
				udfInfoResp: "type=BADTYPE;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUDFResponse(tt.args.udfInfoResp)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseUDFResponse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseUDFResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getUDF(t *testing.T) {
	type args struct {
		node   infoGetter
		policy *a.InfoPolicy
		name   string
	}
	tests := []struct {
		args    args
		want    *models.UDF
		name    string
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				node: newMockInfoGetter(t, "udf-get:filename=test.lua", map[string]string{
					"udf-get:filename=test.lua": "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				}, nil),
				name: "test.lua",
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Content: []byte("function test()\n return 1\n end\n"),
				Name:    "test.lua",
			},
		},
		{
			name: "negative response has wrong command key",
			args: args{
				node: newMockInfoGetter(t, "udf-get:filename=test.lua", map[string]string{
					"bad-key": "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				}, nil),
				name: "test.lua",
			},
			wantErr: true,
		},
		{
			name: "negative infoGetter returned an error",
			args: args{
				node: newMockInfoGetter(t, "udf-get:filename=test.lua", nil, a.ErrConnectionPoolEmpty),
				name: "test.lua",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getUDF(tt.args.node, tt.args.name, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("getUDF() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getUDF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getUDFs(t *testing.T) {
	mockInfoGetter := mocks.NewMockinfoGetter(t)
	mockInfoGetter.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-list").Return(map[string]string{
		"udf-list": "filename=test1.lua;filename=test2.lua;",
	}, nil)
	mockInfoGetter.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-get:filename=test1.lua").Return(map[string]string{
		"udf-get:filename=test1.lua": "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
	}, nil)
	mockInfoGetter.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-get:filename=test2.lua").Return(map[string]string{
		"udf-get:filename=test2.lua": "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte("function test()\n return 2\n end\n")),
	}, nil)

	mockInfoGetterNoUDFs := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoUDFs.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-list").Return(map[string]string{
		"udf-list": "",
	}, nil)

	mockInfoGetterListUDFsFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterListUDFsFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-list").Return(nil, a.ErrNetTimeout)

	mockInfoGetterGetUDFFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetUDFFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-list").Return(map[string]string{
		"udf-list": "filename=test1.lua;",
	}, nil)
	mockInfoGetterGetUDFFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "udf-get:filename=test1.lua").Return(nil, a.ErrNetwork)

	type args struct {
		node   infoGetter
		policy *a.InfoPolicy
	}
	tests := []struct {
		name    string
		args    args
		want    []*models.UDF
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				node: mockInfoGetter,
			},
			want: []*models.UDF{
				{
					UDFType: models.UDFTypeLUA,
					Content: []byte("function test()\n return 1\n end\n"),
					Name:    "test1.lua",
				},
				{
					UDFType: models.UDFTypeLUA,
					Content: []byte("function test()\n return 2\n end\n"),
					Name:    "test2.lua",
				},
			},
		},
		{
			name: "positive no UDFs",
			args: args{
				node: mockInfoGetterNoUDFs,
			},
			want: []*models.UDF(nil),
		},
		{
			name: "negative listing UDFs failed",
			args: args{
				node: mockInfoGetterListUDFsFailed,
			},
			wantErr: true,
		},
		{
			name: "negative getting a UDF failed",
			args: args{
				node: mockInfoGetterGetUDFFailed,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getUDFs(tt.args.node, tt.args.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("getUDFs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getUDFs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetRecordCount(t *testing.T) {
	mockInfoGetter := mocks.NewMockinfoGetter(t)
	mockInfoGetter.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sets/myNamespace").Return(map[string]string{
		"sets/myNamespace": "set=mySet:objects=2",
	}, nil)

	mockInfoGetterNoSets := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoSets.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sets/myNamespace").Return(map[string]string{
		"sets/myNamespace": "",
	}, nil)

	mockInfoGetterReqFail := mocks.NewMockinfoGetter(t)
	mockInfoGetterReqFail.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sets/myNamespace").Return(nil, a.ErrNetTimeout)

	tests := []struct {
		err  error
		name string
		args struct {
			node infoGetter
			sets []string
		}
		want uint64
	}{
		{
			name: "positive with specified sets",
			args: struct {
				node infoGetter
				sets []string
			}{node: mockInfoGetter, sets: []string{"mySet"}},
			want: 2,
		},
		{
			name: "positive with no sets specified",
			args: struct {
				node infoGetter
				sets []string
			}{node: mockInfoGetter, sets: nil},
			want: 2,
		},
		{
			name: "positive with no sets found",
			args: struct {
				node infoGetter
				sets []string
			}{node: mockInfoGetterNoSets, sets: nil},
			want: 0,
		},
		{
			name: "negative request failed",
			args: struct {
				node infoGetter
				sets []string
			}{node: mockInfoGetterReqFail, sets: nil},
			err: a.ErrNetTimeout,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRecordCountForNode(tt.args.node, nil, "myNamespace", tt.args.sets)
			if err != nil && !errors.Is(err, tt.err) {
				t.Errorf("GetRecordCount() error = %v, wantErr %v", err, tt.err)
				return
			}
			if got != tt.want {
				t.Errorf("GetRecordCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseInfoObject(t *testing.T) {
	tests := []struct {
		obj    string
		result infoMap
		err    error
	}{
		{
			"storage-engine.stripe[0]=stripe-0.0xad001000",
			map[string]string{"storage-engine.stripe[0]": "stripe-0.0xad001000"},
			nil,
		},
		{
			"encryption-key-file=secrets:one:two",
			map[string]string{"encryption-key-file": "secrets:one:two"},
			nil,
		},
		{
			"storage-engine1=1:storage-engine2=2",
			map[string]string{"storage-engine1": "1", "storage-engine2": "2"},
			nil,
		},
	}

	for _, tt := range tests {
		result, err := parseInfoObject(tt.obj, ":", "=")
		require.Equal(t, tt.result, result)
		require.Equal(t, tt.err, err)
	}
}

func TestInfoCommander_EnableDisableXDR(t *testing.T) {
	t.Parallel()

	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	err := ic.StartXDR(testASDC, testXDRHostPort, testASNamespace, testASRewind)
	require.NoError(t, err)

	_, err = ic.GetStats(testASDC, testASNamespace)
	require.NoError(t, err)

	err = ic.StopXDR(testASDC)
	require.NoError(t, err)
}

func TestInfoCommander_BlockUnblockMRTWrites(t *testing.T) {
	t.Parallel()
	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	err := ic.BlockMRTWrites(testASNamespace)
	require.NoError(t, err)

	err = ic.UnBlockMRTWrites(testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_parseResultResponse(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		input    map[string]string
		expected string
		errMsg   string
	}{
		{
			name:     "Command exists with successful response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "success"},
			expected: "success",
			errMsg:   "",
		},
		{
			name:     "Command exists with failure response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "ERROR: command failed"},
			expected: "",
			errMsg:   "command testCommand failed: ERROR: command failed",
		},
		{
			name:     "Command not found in map",
			cmd:      "missingCommand",
			input:    map[string]string{"testCommand": "success"},
			expected: "",
			errMsg:   "no response for command missingCommand",
		},
		{
			name:     "Empty response map",
			cmd:      "testCommand",
			input:    map[string]string{},
			expected: "",
			errMsg:   "no response for command testCommand",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseResultResponse(tt.cmd, tt.input)
			if result != tt.expected {
				t.Errorf("expected result %v, got %v", tt.expected, result)
			}
			if err != nil {
				if err.Error() != tt.errMsg {
					t.Errorf("expected error message %v, got %v", tt.errMsg, err)
				}
			} else if tt.errMsg != "" {
				t.Errorf("expected error message %v, got nil", tt.errMsg)
			}
		})
	}
}

func TestInfoCommander_GetSIndexes(t *testing.T) {
	t.Parallel()

	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	_, err := ic.GetSIndexes(testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_GetUDFs(t *testing.T) {
	t.Parallel()

	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	_, err := ic.GetUDFs()
	require.NoError(t, err)
}

func TestInfoCommander_GetRecordCount(t *testing.T) {
	t.Parallel()

	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	_, err := ic.GetRecordCount(testASNamespace, nil)
	require.NoError(t, err)
}

func TestInfoCommander_XDR(t *testing.T) {
	t.Parallel()

	asPolicy := a.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword
	client, aerr := a.NewClientWithPolicy(asPolicy, testASHost, testASPort)
	require.NoError(t, aerr)

	ic := NewInfoClientFromAerospike(client, a.NewInfoPolicy(), models.NewDefaultRetryPolicy())

	err := ic.StartXDR(testASDC, testXDRHostPort, testASNamespace, testASRewind)
	require.NoError(t, err)

	_, err = ic.GetStats(testASDC, testASNamespace)
	require.NoError(t, err)

	err = ic.StopXDR(testASDC)
	require.NoError(t, err)
}
