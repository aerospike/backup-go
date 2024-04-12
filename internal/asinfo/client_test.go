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

package asinfo

import (
	"reflect"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go/internal/asinfo/mocks"
	"github.com/aerospike/backup-go/models"
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
			if got := av.IsGreaterThan(tt.args.other); got != tt.want {
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
		args args
		want string
	}{
		{
			name: "positive no ctx",
			args: args{
				namespace: "test",
				getCtx:    false,
			},
			want: "sindex-list:namespace=test",
		},
		{
			name: "positive with ctx",
			args: args{
				namespace: "test",
				getCtx:    true,
			},
			want: "sindex-list:namespace=test;b64=true",
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
		name    string
		args    args
		want    *models.SIndex
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
		resp string
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
				resp: "foo=bar",
			},
			want: []infoMap{
				{
					"foo": "bar",
				},
			},
		},
		{
			name: "positive value includes '='",
			args: args{
				resp: "foo=bar=as",
			},
			want: []infoMap{
				{
					"foo": "bar=as",
				},
			},
		},
		{
			name: "positive multiple elements",
			args: args{
				resp: "foo=bar:baz=qux",
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
				resp: "foo=bar:baz=qux;bar=foo:qux=bar",
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
				resp: "foo=bar:baz=qux;bar=foo:qux=bar",
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
			name: "negative no data after terminator ;",
			args: args{
				resp: "baz=qux;",
			},
			wantErr: true,
		},
		{
			name: "negative invalid key pair",
			args: args{
				resp: "foo=bar:bazqux;bar=foo:qux=bar",
			},
			wantErr: true,
		},
		{
			name: "negative no syntax",
			args: args{
				resp: "asdfasdfasdf",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInfoResponse(tt.args.resp)
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
	mockInfoGetter := mocks.NewInfoGetter(t)
	mockInfoGetter.On("RequestInfo", (*a.InfoPolicy)(nil), arg).Return(resp, err)
	return mockInfoGetter
}

func Test_getAerospikeVersion(t *testing.T) {
	type args struct {
		node   infoGetter
		policy *a.InfoPolicy
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
	mockInfoGetterNoCtx := mocks.NewInfoGetter(t)
	mockInfoGetterNoCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(
		map[string]string{"build": "5.6.0.0"},
		nil,
	)
	mockInfoGetterNoCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:namespace=test").Return(map[string]string{
		"sindex-list:namespace=test": "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW",
	}, nil)

	mockInfoGetterCtx := mocks.NewInfoGetter(t)
	mockInfoGetterCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(map[string]string{"build": "7.1.0.0"}, nil)
	mockInfoGetterCtx.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:namespace=test;b64=true").Return(map[string]string{
		"sindex-list:namespace=test;b64=true": "ns=test:set=testset:indexname=testindex:bin=testbin:type=numeric:indextype=default:context=AAAAAA==:state=RW",
	}, nil)

	mockInfoGetterGetBuildFailed := mocks.NewInfoGetter(t)
	mockInfoGetterGetBuildFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(nil, a.ErrNetTimeout)

	mockInfoGetterGetSIndexesFailed := mocks.NewInfoGetter(t)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "build").Return(map[string]string{"build": "7.1.0.0"}, nil)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo((*a.InfoPolicy)(nil), "sindex-list:namespace=test;b64=true").Return(nil, a.ErrNetwork)

	type args struct {
		conn      infoGetter
		namespace string
		policy    *a.InfoPolicy
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
			got, err := parseSIndexResponse(tt.args.sindexInfoResp)
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
