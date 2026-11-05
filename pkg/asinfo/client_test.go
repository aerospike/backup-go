// Copyright 2024-2026 Aerospike, Inc.
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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/errclass"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pkg/asinfo/mocks"
	infomodels "github.com/aerospike/backup-go/pkg/asinfo/models"
	"github.com/segmentio/asm/base64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testInfoPolicy is the info policy every test client is built with. Mocked
// infoGetter expectations must match it, because the client passes its own
// policy to RequestInfo instead of taking one per call.
var testInfoPolicy = a.NewInfoPolicy()

// newClient for testing purposes.
// We can't mock version check for dict load,
func newClient(
	cluster NodeGetter,
	policy *a.InfoPolicy,
	retryPolicy *models.RetryPolicy,
) *Client {
	if retryPolicy == nil {
		retryPolicy = models.NewDefaultRetryPolicy()
	}

	ic := &Client{
		cluster:     cluster,
		policy:      policy,
		retryPolicy: retryPolicy,
		logger:      slog.Default(),
	}

	ic.cmds = newInfoCommands(infomodels.AerospikeVersionRecentInfoCommands)

	return ic
}

func Test_parseAerospikeVersion(t *testing.T) {
	t.Parallel()
	type args struct {
		versionStr string
	}
	tests := []struct {
		name    string
		args    args
		want    infomodels.AerospikeVersion
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				versionStr: "5.6.0.0",
			},
			want: infomodels.AerospikeVersion{
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
			want: infomodels.AerospikeVersion{
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
			want: infomodels.AerospikeVersion{
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
			t.Parallel()
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
	t.Parallel()
	type fields struct {
		Major int
		Minor int
		Patch int
	}
	type args struct {
		other infomodels.AerospikeVersion
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
			t.Parallel()
			av := infomodels.AerospikeVersion{
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

func Test_parseSIndex(t *testing.T) {
	t.Parallel()
	cdtCtx, err := a.CDTContextToBase64([]*a.CDTContext{a.CtxListValue(a.NewValue([]byte("hi")))})
	if err != nil {
		panic(err)
	}

	type args struct {
		sindexMap infomodels.InfoMap
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeNone,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeMapValues,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeMapValues,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "null",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeNumeric,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeIntSigned,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "string",
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "text",
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "blob",
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeo2DSphere,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeoJSON,
					"indextype": indexTypeDefault,
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
				sindexMap: infomodels.InfoMap{
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeoJSON,
					"indextype": indexTypeDefault,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing indexname",
			args: args{
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeoJSON,
					"indextype": indexTypeDefault,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing indextype",
			args: args{
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeoJSON,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing bin",
			args: args{
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"type":      indexBinTypeGeoJSON,
					"indextype": indexTypeDefault,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative response missing type",
			args: args{
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"indextype": indexTypeDefault,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "negative invalid indextype",
			args: args{
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      indexBinTypeGeoJSON,
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
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"bin":       "testbin",
					"type":      "badtype",
					"indextype": indexTypeDefault,
					"context":   "null",
					"state":     "RW",
				},
			},
			wantErr: true,
		},
		{
			name: "set index type",
			args: args{
				// map[indexname:set_idx indextype:set ns:source-ns1 set:testRacks]]
				sindexMap: infomodels.InfoMap{
					"ns":        "test",
					"indexname": "testindex",
					"set":       "testset",
					"indextype": indexTypeSet,
				},
			},
			want: &models.SIndex{
				Namespace: "test",
				Name:      "testindex",
				Set:       "testset",
				IndexType: models.SetSIndex,
				Path:      models.NewEmptySIndexPath(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseSIndex(tt.args.sindexMap)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.name == "negative invalid indextype" {
				require.ErrorIs(t, err, ErrInvalidSIndexType)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseInfoResponse(t *testing.T) {
	t.Parallel()
	type args struct {
		resp    string
		objSep  string
		pairSep string
		kvSep   string
	}
	tests := []struct {
		name    string
		args    args
		want    []infomodels.InfoMap
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
			want: []infomodels.InfoMap{
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
			want: []infomodels.InfoMap{
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
			want: []infomodels.InfoMap{
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
			want: []infomodels.InfoMap{
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
			want: []infomodels.InfoMap{
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
			want: []infomodels.InfoMap{
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
			t.Parallel()
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
	mockInfoGetter.On("RequestInfo", testInfoPolicy, []string{arg}).Return(resp, err)
	return mockInfoGetter
}

func Test_getAerospikeVersion(t *testing.T) {
	t.Parallel()

	type args struct {
		node infoGetter
	}
	tests := []struct {
		args    args
		name    string
		want    infomodels.AerospikeVersion
		wantErr bool
	}{
		{
			name: "positive simple",
			args: args{
				node: newMockInfoGetter(t, cmdBuild, map[string]string{cmdBuild: "5.6.0.0"}, nil),
			},
			want: infomodels.AerospikeVersion{
				Major: 5,
				Minor: 6,
				Patch: 0,
			},
		},
		{
			name: "positive dev build",
			args: args{
				node: newMockInfoGetter(t, cmdBuild, map[string]string{cmdBuild: "7.6.1.0-rc2-ghasd"}, nil),
			},
			want: infomodels.AerospikeVersion{
				Major: 7,
				Minor: 6,
				Patch: 1,
			},
		},
		{
			name: "negative request info fails",
			args: args{
				node: newMockInfoGetter(t, cmdBuild, nil, a.ErrNetTimeout),
			},
			wantErr: true,
		},
		{
			name: "negative command response reports an error",
			args: args{
				node: newMockInfoGetter(t, cmdBuild, map[string]string{cmdBuild: errCmdRespPrefix + ": unknown command"}, nil),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			mockNodeGetter.EXPECT().GetRandomNode().Return(&a.Node{}, nil).Maybe()

			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

			got, err := ic.getAerospikeVersion(tt.args.node)
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

// Test_getAerospikeVersion_errorMessage covers every way the build request can
// fail. All of them are reported by the same message, which must name the
// operation and must not claim the response was parsed.
func Test_getAerospikeVersion_errorMessage(t *testing.T) {
	t.Parallel()

	const wantMsg = "failed to get build version"

	tests := []struct {
		resp   map[string]string
		reqErr a.Error
		name   string
	}{
		{
			name:   "request fails",
			reqErr: a.ErrNetTimeout,
		},
		{
			name: "command response reports an error",
			resp: map[string]string{cmdBuild: errCmdRespPrefix + ": unknown command"},
		},
		{
			name: "response misses the command key",
			resp: map[string]string{"bad-key": "7.1.0.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := newMockInfoGetter(t, cmdBuild, tt.resp, tt.reqErr)
			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			_, err := ic.getAerospikeVersion(node)
			require.Error(t, err)
			require.ErrorIs(t, err, errclass.ErrAerospike)
			assert.Contains(t, err.Error(), wantMsg)
		})
	}
}

// TestClient_randomNodeErrorIsPreserved guards the report of a failed cluster
// node lookup. A const aerospike error wraps nothing, so unwrapping it before
// reporting dropped the message and left a formatting placeholder instead.
func TestClient_randomNodeErrorIsPreserved(t *testing.T) {
	t.Parallel()

	const (
		testNamespace = "source-ns1"
		wantMsg       = "cluster is empty"
	)

	tests := []struct {
		run  func(ctx context.Context, ic *Client) error
		name string
	}{
		{
			name: "GetSIndexes",
			run: func(ctx context.Context, ic *Client) error {
				_, err := ic.GetSIndexes(ctx, testNamespace)

				return err
			},
		},
		{
			name: "GetUDFs",
			run: func(ctx context.Context, ic *Client) error {
				_, err := ic.GetUDFs(ctx)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			mockNodeGetter.EXPECT().GetRandomNode().Return(nil, a.ErrClusterIsEmpty)

			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(0, 1, 1))

			err := tt.run(t.Context(), ic)
			require.Error(t, err)
			require.ErrorIs(t, err, errclass.ErrAerospike)
			assert.Contains(t, err.Error(), wantMsg)
		})
	}
}

func Test_getSIndexes(t *testing.T) {
	t.Parallel()

	const (
		testNamespace   = "test"
		testBuildNoCtx  = "5.6.0.0"
		testBuildCtx    = "7.1.0.0"
		testSIndexB64   = "AAAAAA=="
		testSIndexNoCtx = "ns=test:set=testset:indexname=testindex:bin=testbin:" +
			"type=numeric:indextype=default:context=null:state=RW"
		testSIndexWithCtx = "ns=test:set=testset:indexname=testindex:bin=testbin:" +
			"type=numeric:indextype=default:context=" + testSIndexB64 + ":state=RW"
		cmdSIndexNoCtx   = "sindex-list:namespace=test"
		cmdSIndexWithCtx = "sindex-list:namespace=test;b64=true"
	)

	mockInfoGetterNoCtx := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoCtx.EXPECT().RequestInfo(testInfoPolicy, []string{cmdBuild}).Return(
		map[string]string{cmdBuild: testBuildNoCtx},
		nil,
	)
	mockInfoGetterNoCtx.EXPECT().RequestInfo(testInfoPolicy, []string{cmdSIndexNoCtx}).Return(map[string]string{
		cmdSIndexNoCtx: testSIndexNoCtx,
	}, nil)

	mockInfoGetterCtx := mocks.NewMockinfoGetter(t)
	mockInfoGetterCtx.EXPECT().RequestInfo(testInfoPolicy, []string{cmdBuild}).
		Return(map[string]string{cmdBuild: testBuildCtx}, nil)
	mockInfoGetterCtx.EXPECT().RequestInfo(testInfoPolicy, []string{cmdSIndexWithCtx}).Return(map[string]string{
		cmdSIndexWithCtx: testSIndexWithCtx,
	}, nil)

	mockInfoGetterGetBuildFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetBuildFailed.EXPECT().RequestInfo(testInfoPolicy, []string{cmdBuild}).Return(nil, a.ErrNetTimeout)

	mockInfoGetterGetSIndexesFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo(testInfoPolicy, []string{cmdBuild}).
		Return(map[string]string{cmdBuild: testBuildCtx}, nil)
	mockInfoGetterGetSIndexesFailed.EXPECT().RequestInfo(testInfoPolicy, []string{cmdSIndexWithCtx}).
		Return(nil, a.ErrNetwork)

	type args struct {
		conn      infoGetter
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
				namespace: testNamespace,
			},
			want: []*models.SIndex{
				{
					Namespace: testNamespace,
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
				namespace: testNamespace,
			},
			want: []*models.SIndex{
				{
					Namespace: testNamespace,
					Name:      "testindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName:    "testbin",
						BinType:    models.NumericSIDataType,
						B64Context: testSIndexB64,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
		{
			name: "negative get build fails",
			args: args{
				conn:      mockInfoGetterGetBuildFailed,
				namespace: testNamespace,
			},
			wantErr: true,
		},
		{
			name: "negative get sindex fails",
			args: args{
				conn:      mockInfoGetterGetSIndexesFailed,
				namespace: testNamespace,
			},
			wantErr: true,
		},
	}

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetRandomNode().Return(&a.Node{}, nil).Maybe()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

			got, err := ic.requestSIndexes(tt.args.conn, tt.args.namespace, false)
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
	t.Parallel()
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
			name: "negative bad sindex bin type",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=testindex:bin=testbin:type=BADTYPE:indextype=default:context=null:state=RW",
			},
			wantErr: true,
		},
		{
			name: "positive skips invalid indextype",
			args: args{
				sindexInfoResp: "ns=test:set=testset:indexname=validindex:bin=testbin:type=numeric:indextype=default:context=null:state=RW;ns=test:set=testset:indexname=badindex:bin=testbin:type=numeric:indextype=badtype:context=null:state=RW",
			},
			want: []*models.SIndex{
				{
					Namespace: "test",
					Name:      "validindex",
					Set:       "testset",
					Path: models.SIndexPath{
						BinName: "testbin",
						BinType: models.NumericSIDataType,
					},
					IndexType: models.BinSIndex,
				},
			},
		},
	}
	mockNodeGetter := mocks.NewMockNodeGetter(t)
	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ic.parseSIndexes(tt.args.sindexInfoResp, false)
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
	t.Parallel()
	type fields struct {
		Major int
		Minor int
		Patch int
	}
	type args struct {
		other infomodels.AerospikeVersion
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
				other: infomodels.AerospikeVersion{
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
			t.Parallel()
			av := infomodels.AerospikeVersion{
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
	t.Parallel()
	type args struct {
		udfMap infomodels.InfoMap
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
				udfMap: infomodels.InfoMap{
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
				udfMap: infomodels.InfoMap{
					"content": base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				},
			},
			wantErr: true,
		},
		{
			name: "negative bad language type",
			args: args{
				udfMap: infomodels.InfoMap{
					"type":    "BADTYPE",
					"content": base64.StdEncoding.EncodeToString([]byte("function test()\n return 1\n end\n")),
				},
			},
			wantErr: true,
		},
		{
			name: "negative missing content",
			args: args{
				udfMap: infomodels.InfoMap{
					"type": "LUA",
				},
			},
			wantErr: true,
		},
		{
			name: "negative content is not base64 encoded",
			args: args{
				udfMap: infomodels.InfoMap{
					"type":    "LUA",
					"content": "function test()\n return 1\n end\n",
				},
			},
			wantErr: true,
		},
		{
			name: "negative wrong number of info elements",
			args: args{
				udfMap: infomodels.InfoMap{
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
			t.Parallel()
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
	t.Parallel()
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
			t.Parallel()
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
	t.Parallel()

	const (
		testUDFName    = "test.lua"
		testUDFContent = "function test()\n return 1\n end\n"
		testUDFCmd     = "udf-get:filename=test.lua"
	)

	testUDFResp := "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte(testUDFContent))

	type args struct {
		node infoGetter
		name string
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
				node: newMockInfoGetter(t, testUDFCmd, map[string]string{
					testUDFCmd: testUDFResp,
				}, nil),
				name: testUDFName,
			},
			want: &models.UDF{
				UDFType: models.UDFTypeLUA,
				Content: []byte(testUDFContent),
				Name:    testUDFName,
			},
		},
		{
			name: "negative response has wrong command key",
			args: args{
				node: newMockInfoGetter(t, testUDFCmd, map[string]string{
					"bad-key": testUDFResp,
				}, nil),
				name: testUDFName,
			},
			wantErr: true,
		},
		{
			name: "negative infoGetter returned an error",
			args: args{
				node: newMockInfoGetter(t, testUDFCmd, nil, a.ErrConnectionPoolEmpty),
				name: testUDFName,
			},
			wantErr: true,
		},
		{
			name: "negative command response reports an error",
			args: args{
				node: newMockInfoGetter(t, testUDFCmd, map[string]string{
					testUDFCmd: errCmdRespPrefix + ": file not found",
				}, nil),
				name: testUDFName,
			},
			wantErr: true,
		},
	}

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetRandomNode().Return(&a.Node{}, nil).Maybe()

	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ic.getUDF(tt.args.node, tt.args.name)
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

func Test_getUDF_InvalidName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		giveName string
	}{
		{name: "empty name"},
		{name: "name with separator", giveName: "test.lua;restore-policy=fuzzy"},
	}

	ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ic.getUDF(mocks.NewMockinfoGetter(t), tt.giveName)

			require.ErrorIs(t, err, errclass.ErrAerospike)
			require.NotErrorIs(t, err, errclass.ErrInvalidConfig)
			assert.Nil(t, got)
		})
	}
}

func Test_getUDFs(t *testing.T) {
	t.Parallel()

	const (
		testUDF1Name    = "test1.lua"
		testUDF2Name    = "test2.lua"
		testUDF1Content = "function test()\n return 1\n end\n"
		testUDF2Content = "function test()\n return 2\n end\n"
		testUDF1Cmd     = "udf-get:filename=test1.lua"
		testUDF2Cmd     = "udf-get:filename=test2.lua"
	)

	var (
		testUDF1Resp = "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte(testUDF1Content))
		testUDF2Resp = "type=LUA;content=" + base64.StdEncoding.EncodeToString([]byte(testUDF2Content))
	)

	mockInfoGetter := mocks.NewMockinfoGetter(t)
	mockInfoGetter.EXPECT().RequestInfo(testInfoPolicy, []string{cmdUdfList}).Return(map[string]string{
		cmdUdfList: "filename=" + testUDF1Name + ";filename=" + testUDF2Name + ";",
	}, nil)
	mockInfoGetter.EXPECT().RequestInfo(testInfoPolicy, []string{testUDF1Cmd}).Return(map[string]string{
		testUDF1Cmd: testUDF1Resp,
	}, nil)
	mockInfoGetter.EXPECT().RequestInfo(testInfoPolicy, []string{testUDF2Cmd}).Return(map[string]string{
		testUDF2Cmd: testUDF2Resp,
	}, nil)

	mockInfoGetterNoUDFs := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoUDFs.EXPECT().RequestInfo(testInfoPolicy, []string{cmdUdfList}).Return(map[string]string{
		cmdUdfList: "",
	}, nil)

	mockInfoGetterListUDFsFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterListUDFsFailed.EXPECT().RequestInfo(testInfoPolicy, []string{cmdUdfList}).Return(nil, a.ErrNetTimeout)

	mockInfoGetterListUDFsRespErr := mocks.NewMockinfoGetter(t)
	mockInfoGetterListUDFsRespErr.EXPECT().RequestInfo(testInfoPolicy, []string{cmdUdfList}).Return(map[string]string{
		cmdUdfList: errCmdRespPrefix + ": not authenticated",
	}, nil)

	mockInfoGetterGetUDFFailed := mocks.NewMockinfoGetter(t)
	mockInfoGetterGetUDFFailed.EXPECT().RequestInfo(testInfoPolicy, []string{cmdUdfList}).Return(map[string]string{
		cmdUdfList: "filename=" + testUDF1Name + ";",
	}, nil)
	mockInfoGetterGetUDFFailed.EXPECT().RequestInfo(testInfoPolicy, []string{testUDF1Cmd}).Return(nil, a.ErrNetwork)

	type args struct {
		node infoGetter
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
					Content: []byte(testUDF1Content),
					Name:    testUDF1Name,
				},
				{
					UDFType: models.UDFTypeLUA,
					Content: []byte(testUDF2Content),
					Name:    testUDF2Name,
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
			name: "negative udf-list response reports an error",
			args: args{
				node: mockInfoGetterListUDFsRespErr,
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

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetRandomNode().Return(&a.Node{}, nil).Maybe()

	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ic.getUDFs(tt.args.node)
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
	t.Parallel()

	const (
		testNamespace = "myNamespace"
		testSet       = "mySet"
		testSetsCmd   = "sets/myNamespace"
	)

	mockInfoGetter := mocks.NewMockinfoGetter(t)
	mockInfoGetter.EXPECT().RequestInfo(testInfoPolicy, []string{testSetsCmd}).Return(map[string]string{
		testSetsCmd: "set=" + testSet + ":objects=2",
	}, nil)

	mockInfoGetterNoSets := mocks.NewMockinfoGetter(t)
	mockInfoGetterNoSets.EXPECT().RequestInfo(testInfoPolicy, []string{testSetsCmd}).Return(map[string]string{
		testSetsCmd: "",
	}, nil)

	mockInfoGetterReqFail := mocks.NewMockinfoGetter(t)
	mockInfoGetterReqFail.EXPECT().RequestInfo(testInfoPolicy, []string{testSetsCmd}).Return(nil, a.ErrNetTimeout)

	mockInfoGetterRespErr := mocks.NewMockinfoGetter(t)
	mockInfoGetterRespErr.EXPECT().RequestInfo(testInfoPolicy, []string{testSetsCmd}).Return(map[string]string{
		testSetsCmd: errCmdRespPrefix + ": unknown namespace",
	}, nil)

	type args struct {
		node infoGetter
		sets []string
	}
	tests := []struct {
		err  error
		name string
		args args
		want uint64
	}{
		{
			name: "positive with specified sets",
			args: args{node: mockInfoGetter, sets: []string{testSet}},
			want: 2,
		},
		{
			name: "positive with no sets specified",
			args: args{node: mockInfoGetter, sets: nil},
			want: 2,
		},
		{
			name: "positive with no sets found",
			args: args{node: mockInfoGetterNoSets, sets: nil},
			want: 0,
		},
		{
			name: "negative request failed",
			args: args{node: mockInfoGetterReqFail, sets: nil},
			err:  a.ErrNetTimeout,
		},
		{
			name: "negative command response reports an error",
			args: args{node: mockInfoGetterRespErr, sets: nil},
			err:  errclass.ErrAerospike,
		},
	}

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetRandomNode().Return(&a.Node{}, nil).Maybe()

	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ic.getRecordCountForNode(tt.args.node, testSetsCmd, tt.args.sets)
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
	t.Parallel()
	tests := []struct {
		obj    string
		result infomodels.InfoMap
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

func TestClient_parseResultResponse(t *testing.T) {
	t.Parallel()
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
			name:     "Command exists with a successful response holding ERROR",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "job-id=1:error-reason=S3 ERROR:2:slow down"},
			expected: "job-id=1:error-reason=S3 ERROR:2:slow down",
			errMsg:   "",
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
			t.Parallel()
			result, err := parseResultResponse(tt.cmd, tt.input)

			if tt.errMsg == "" {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)

				return
			}

			// The class is the contract, the message text is not.
			require.ErrorIs(t, err, errclass.ErrAerospike)
			require.ErrorContains(t, err, tt.errMsg)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestClient_GetBackupStatus(t *testing.T) {
	t.Parallel()

	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetNodes().Return([]*a.Node{})

	ic := newClient(mockNodeGetter, testInfoPolicy, models.NewDefaultRetryPolicy())

	ctx := t.Context()

	_, err := ic.GetBackupStatus(ctx, "523607479")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestClient_AbortServerBackup(t *testing.T) {
	t.Parallel()

	const testBackupID = "523607479"

	// The command sent to the principal must carry the job being aborted.
	ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
	cmd, err := ic.cmds.backupAbort(testBackupID)
	require.NoError(t, err)
	require.Equal(t, "backup-abort:job-id="+testBackupID, cmd)

	// The principal is needed before the abort can be sent, so a cluster that
	// hands out no node must surface as an Aerospike-class error.
	mockNodeGetter := mocks.NewMockNodeGetter(t)
	mockNodeGetter.EXPECT().GetRandomNode().Return(nil, a.ErrInvalidParam).Maybe()

	ic = newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(0, 1, 1))

	err = ic.AbortBackup(t.Context(), testBackupID)

	require.ErrorIs(t, err, errclass.ErrAerospike)
	require.ErrorContains(t, err, "failed to get cluster principal")
}

// Shared by the AbortRestore tests below.
const (
	testAbortRestoreNamespace = "source-ns1"
	testAbortRestoreJobID     = testCmdJobID
)

// TestClient_AbortRestore covers the failure paths reachable without a live cluster.
// The abort is sent to the cluster principal, so everything that happens before the
// command reaches a node is observable here.
func TestClient_AbortRestore(t *testing.T) {
	t.Parallel()

	const testMaxRetries = 2

	tests := []struct {
		setup      func(t *testing.T) (*Client, context.Context)
		wantErrIs  error
		name       string
		wantErrMsg string
	}{
		{
			name: "principal is unreachable",
			setup: func(t *testing.T) (*Client, context.Context) {
				t.Helper()

				mockNodeGetter := mocks.NewMockNodeGetter(t)
				mockNodeGetter.EXPECT().GetRandomNode().Return(nil, a.ErrInvalidParam)

				ic := newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(0, 1, testMaxRetries))

				return ic, t.Context()
			},
			wantErrIs:  errclass.ErrAerospike,
			wantErrMsg: "failed to get cluster principal",
		},
		{
			name: "canceled context stops the retries",
			setup: func(t *testing.T) (*Client, context.Context) {
				t.Helper()

				mockNodeGetter := mocks.NewMockNodeGetter(t)
				mockNodeGetter.EXPECT().GetRandomNode().Return(nil, a.ErrInvalidParam)

				// The backoff must be long enough for the canceled context, and not
				// the elapsed delay, to end the wait between the attempts.
				ic := newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(time.Hour, 1, testMaxRetries))

				ctx, cancel := context.WithCancel(t.Context())
				cancel()

				return ic, ctx
			},
			wantErrIs: context.Canceled,
		},
		{
			name: "nil retry policy is rejected before any request",
			setup: func(t *testing.T) (*Client, context.Context) {
				t.Helper()

				ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
				ic.retryPolicy = nil

				return ic, t.Context()
			},
			wantErrIs:  errclass.ErrInvalidConfig,
			wantErrMsg: "retry policy cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic, ctx := tt.setup(t)

			err := ic.AbortRestore(ctx, testAbortRestoreNamespace, testAbortRestoreJobID)

			require.ErrorIs(t, err, tt.wantErrIs)

			if tt.wantErrMsg != "" {
				require.ErrorContains(t, err, tt.wantErrMsg)
			}
		})
	}
}

// backupJob builds a minimal InfoMap representing a backup job.
func backupJob(trid, timeSinceDone, progress, pids string) infomodels.InfoMap {
	return infomodels.InfoMap{
		"trid":             trid,
		"ns":               "test-ns",
		"job-type":         jobTypeBackup,
		"time-since-done":  timeSinceDone,
		"job-progress":     progress,
		"n-pids-requested": pids,
	}
}

func TestFilterBackupsSortedByTimeSinceDone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     []infomodels.InfoMap
		wantTRIDs []string
		wantErr   bool
	}{
		{
			name:      "empty input",
			input:     nil,
			wantTRIDs: []string{},
		},
		{
			name: "filters out non-backup jobs",
			input: []infomodels.InfoMap{
				{"trid": "1", "job-type": "scan", "time-since-done": "100"},
				{"trid": "2", "job-type": "query", "time-since-done": "50"},
				backupJob("3", "200", "100", "2048"),
			},
			wantTRIDs: []string{"3"},
		},
		{
			name: "ascending by time-since-done",
			input: []infomodels.InfoMap{
				backupJob("1", "300", "20", "1024"),
				backupJob("2", "100", "40", "2048"),
				backupJob("3", "200", "100", "4096"),
			},
			wantTRIDs: []string{"2", "3", "1"},
		},
		{
			name: "missing time-since-done goes to the end",
			input: []infomodels.InfoMap{
				backupJob("1", "300", "50", "2048"),
				{"trid": "2", "job-type": jobTypeBackup},
				backupJob("3", "100", "100", "4096"),
			},
			wantTRIDs: []string{"3", "1", "2"},
		},
		{
			name: "all jobs filtered out",
			input: []infomodels.InfoMap{
				{"trid": "1", "job-type": "scan", "time-since-done": "100"},
			},
			wantTRIDs: []string{},
		},
		{
			name: "malformed time-since-done returns error",
			input: []infomodels.InfoMap{
				backupJob("1", "not-a-number", "100", "4096"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := filterBackupsSortedByTimeSinceDone(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			gotTRIDs := make([]string, len(got))
			for i, j := range got {
				gotTRIDs[i] = j["trid"]
			}
			assert.Equal(t, tt.wantTRIDs, gotTRIDs)
		})
	}
}

func newTestClient(t *testing.T) *Client {
	t.Helper()
	return &Client{
		policy:      testInfoPolicy,
		retryPolicy: models.NewDefaultRetryPolicy(),
		cmds:        newInfoCommands(infomodels.AerospikeVersionSupportsIntegratedBackup),
	}
}

func TestClient_getBackupStatusByNode(t *testing.T) {
	t.Parallel()

	const (
		testJobID       = testCmdJobID
		testNamespace   = "source-ns1"
		backupStatusCmd = testBackupStatusCmd
	)

	const runningStatus = "job-id=260922T103653-5xsr:ns=source-ns1:state=INCR_SCAN_ACTIVE:" +
		"backup-rps=0:backup-worker-threads=8:backup-upload-threads=8:backup-s3-throughput-target-gbps=10:" +
		"recs-backed-up=515423:recs-base=515423:recs-query-incr=0:recs-change-incr=0:" +
		"bytes-backed-up=1048576:bytes-base=1048576:bytes-query-incr=0:bytes-change-incr=0:" +
		"recs-filtered=0:recs-degenerate=0:recs-skipped-xdr-tomb=0:recs-read-failed=0:" +
		"partitions-flushed=1365:partitions-owned=1365:partitions-scanned=1365:" +
		"progress-pct=95.00:change-stream-active=true:start-time=2026-09-22 10.36.53.415 UTC:finish-time=-:" +
		"partitions-count-pending=0:count-read-failures=0:backpressure-active=true:" +
		"backpressure-activations=3:bytes-in-per-sec=2048:bytes-out-per-sec=1024:upload-q-depth=4:" +
		"upload-q-cap=64:buffers-in-use=12:total-buffers-allocated=32"

	const failedStatus = "job-id=260922T103653-5xsr:ns=source-ns1:state=ABORTED:recs-backed-up=12:" +
		"recs-base=10:recs-query-incr=1:recs-change-incr=1:" +
		"start-time=2026-09-22 10.36.53.415 UTC:finish-time=2026-09-22 10.41.20.077 UTC:" +
		"error-reason=storage unreachable:abort-origin=server"

	tests := []struct {
		name     string
		response string
		reqErr   a.Error
		wantErr  bool
		check    func(t *testing.T, resp []infomodels.InfoMap)
	}{
		{
			name:     "parses running backup status response",
			response: runningStatus,
			check: func(t *testing.T, resp []infomodels.InfoMap) {
				t.Helper()

				status := infomodels.NewResponseBackupState(resp)
				require.NotNil(t, status)
				assert.Equal(t, infomodels.BackupStateIncrScanActive, status.State)
				assert.Equal(t, testJobID, status.JobID)
				assert.Equal(t, testNamespace, status.Namespace)
				assert.Equal(t, uint64(515423), status.RecsBase)
				assert.Equal(t, uint64(1048576), status.BytesBackedUp)
				assert.Equal(t, uint32(8), status.BackupWorkerThreads)
				assert.Equal(t, uint32(10), status.BackupS3ThroughputTargetGbps)
				assert.Equal(t, uint32(1365), status.PartitionsOwned)
				assert.InEpsilon(t, 95.0, status.ProgressPct, 0.01)
				assert.True(t, status.ChangeStreamActive)
				assert.True(t, status.BackpressureActive)
				assert.Equal(t, uint64(32), status.TotalBuffersAllocated)
				assert.Equal(t, time.Date(2026, 9, 22, 10, 36, 53, 415000000, time.UTC), status.StartTime)
				assert.True(t, status.FinishTime.IsZero())
			},
		},
		{
			name:     "parses conditional error-reason and abort-origin fields",
			response: failedStatus,
			check: func(t *testing.T, resp []infomodels.InfoMap) {
				t.Helper()

				status := infomodels.NewResponseBackupState(resp)
				require.NotNil(t, status)
				assert.Equal(t, infomodels.BackupStateAborted, status.State)
				assert.Equal(t, "storage unreachable", status.ErrorReason)
				assert.Equal(t, infomodels.AbortOriginServer, status.AbortOrigin)
				assert.Equal(t, uint64(12), status.RecsBackedUp)
				assert.Equal(t, uint64(1), status.RecsQueryIncr)
				assert.Equal(t, uint64(1), status.RecsChangeIncr)
				assert.False(t, status.FinishTime.IsZero())
			},
		},
		{
			name:     "empty response",
			response: "",
			check: func(t *testing.T, resp []infomodels.InfoMap) {
				t.Helper()

				assert.Nil(t, resp)
				assert.Nil(t, infomodels.NewResponseBackupState(resp))
			},
		},
		{
			name:    "request info error",
			reqErr:  a.ErrInvalidParam,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := newMockInfoGetter(
				t,
				backupStatusCmd,
				map[string]string{backupStatusCmd: tt.response},
				tt.reqErr,
			)

			ic := newTestClient(t)
			resp, err := ic.getBackupStatusByNode(node, backupStatusCmd)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, resp)
			}
		})
	}
}

func TestClient_getBackupStatusByNode_RequestInfoError(t *testing.T) {
	t.Parallel()

	const backupStatusCmd = "backup-status:job-id=523607479"

	node := newMockInfoGetter(t, backupStatusCmd, nil, a.ErrInvalidParam)

	ic := newTestClient(t)
	_, err := ic.getBackupStatusByNode(node, backupStatusCmd)
	require.Error(t, err)

	assert.Contains(t, err.Error(), "failed to request backup status")
	assert.ErrorIs(t, err, errclass.ErrAerospike)
}

// Shared by the integrated backup client tests below.
const (
	testIntegratedJobID     = "523607479"
	testIntegratedNamespace = "test"
	testIntegratedStorage   = "aws-s3"

	testMethodStartBackup         = "StartBackup"
	testMethodStartRestore        = "StartRestore"
	testMethodAbortBackup         = "AbortBackup"
	testMethodAbortRestore        = "AbortRestore"
	testMethodOpenRestore         = "OpenRestore"
	testMethodClearRestoreStorage = "ClearRestoreStorage"
	testMethodGetBackupStatus     = "GetBackupStatus"
	testMethodGetRestoreStatus    = "GetRestoreStatus"
)

// clientCall runs one Client method under test.
type clientCall struct {
	name string
	call func(ctx context.Context, ic *Client) error
}

// startBackupCall calls StartBackup with the given request fields.
func startBackupCall(namespace string, storage infomodels.StorageType) clientCall {
	return clientCall{
		name: testMethodStartBackup,
		call: func(ctx context.Context, ic *Client) error {
			_, err := ic.StartBackup(ctx, &infomodels.RequestBackup{
				RequestCommon: infomodels.RequestCommon{Namespace: namespace, Storage: storage},
			})

			return err
		},
	}
}

// startRestoreCall calls StartRestore with the given request fields.
func startRestoreCall(backupID, namespace string, storage infomodels.StorageType) clientCall {
	return clientCall{
		name: testMethodStartRestore,
		call: func(ctx context.Context, ic *Client) error {
			_, err := ic.StartRestore(ctx, &infomodels.RequestRestore{
				RequestCommon: infomodels.RequestCommon{Namespace: namespace, Storage: storage},
				BackupIDs:     backupID,
			})

			return err
		},
	}
}

// abortBackupCall calls AbortBackup for jobID.
func abortBackupCall(jobID string) clientCall {
	return clientCall{
		name: testMethodAbortBackup,
		call: func(ctx context.Context, ic *Client) error {
			return ic.AbortBackup(ctx, jobID)
		},
	}
}

// abortRestoreCall calls AbortRestore for jobID of namespace.
func abortRestoreCall(jobID, namespace string) clientCall {
	return clientCall{
		name: testMethodAbortRestore,
		call: func(ctx context.Context, ic *Client) error {
			return ic.AbortRestore(ctx, namespace, jobID)
		},
	}
}

// openRestoreCall calls OpenRestore for jobID of namespace.
func openRestoreCall(jobID, namespace string) clientCall {
	return clientCall{
		name: testMethodOpenRestore,
		call: func(ctx context.Context, ic *Client) error {
			return ic.OpenRestore(ctx, namespace, jobID)
		},
	}
}

// integratedBackupClientCalls returns every integrated backup method of Client
// called with the given arguments.
func integratedBackupClientCalls(jobID, backupID, namespace string, storage infomodels.StorageType,
) []clientCall {
	return []clientCall{
		startBackupCall(namespace, storage),
		startRestoreCall(backupID, namespace, storage),
		abortBackupCall(jobID),
		abortRestoreCall(jobID, namespace),
		openRestoreCall(jobID, namespace),
		{
			name: testMethodClearRestoreStorage,
			call: func(ctx context.Context, ic *Client) error {
				return ic.ClearRestoreStorage(ctx, namespace, jobID)
			},
		},
		{
			name: testMethodGetBackupStatus,
			call: func(ctx context.Context, ic *Client) error {
				_, err := ic.GetBackupStatus(ctx, jobID)

				return err
			},
		},
		{
			name: testMethodGetRestoreStatus,
			call: func(ctx context.Context, ic *Client) error {
				_, err := ic.GetRestoreStatus(ctx, namespace, jobID)

				return err
			},
		},
	}
}

// startJobCall calls a Client method that starts a job and returns its job id.
type startJobCall struct {
	call func(ctx context.Context, ic *Client, namespace string) (string, error)
	name string
}

// startJobCalls returns the Client methods that start a job, StartBackup and
// StartRestore, called for namespace with the other required fields set.
func startJobCalls() []startJobCall {
	return []startJobCall{
		{
			name: testMethodStartBackup,
			call: func(ctx context.Context, ic *Client, namespace string) (string, error) {
				return ic.StartBackup(ctx, &infomodels.RequestBackup{
					RequestCommon: infomodels.RequestCommon{Namespace: namespace, Storage: testIntegratedStorage},
				})
			},
		},
		{
			name: testMethodStartRestore,
			call: func(ctx context.Context, ic *Client, namespace string) (string, error) {
				return ic.StartRestore(ctx, &infomodels.RequestRestore{
					RequestCommon: infomodels.RequestCommon{Namespace: namespace, Storage: testIntegratedStorage},
					BackupIDs:     testCmdBackupID,
				})
			},
		},
	}
}

// TestClient_StartJobID checks that StartBackup and StartRestore generate a new
// job id on every call and return it even when the start fails on the cluster,
// so that the caller can look the job up by it: the server may have accepted
// the job before the start failed.
func TestClient_StartJobID(t *testing.T) {
	t.Parallel()

	const testCalls = 3

	for _, tt := range startJobCalls() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			mockNodeGetter.EXPECT().GetRandomNode().Return(nil, a.ErrInvalidParam).Times(testCalls)

			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(0, 1, 1))

			ids := make(map[string]struct{}, testCalls)

			for range testCalls {
				id, err := tt.call(t.Context(), ic, testIntegratedNamespace)

				require.ErrorIs(t, err, errclass.ErrAerospike)
				require.True(t, IsJobID(id), "generated id has an unexpected form: %q", id)

				ids[id] = struct{}{}
			}

			assert.Len(t, ids, testCalls)
		})
	}
}

// TestClient_StartJobIDInvalidRequest checks that StartBackup and StartRestore
// return no job id for a request rejected before any node is asked: no job can
// exist then. The mocked cluster has no expectations, so any request to it
// fails the test.
func TestClient_StartJobIDInvalidRequest(t *testing.T) {
	t.Parallel()

	for _, tt := range startJobCalls() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			id, err := tt.call(t.Context(), ic, "")

			require.ErrorIs(t, err, errMissingCmdParam)
			assert.Empty(t, id)
		})
	}
}

// TestClient_RetriesAreNotNested guards the commands that look the cluster principal
// up before sending their own command. Both the lookup and the command used to be
// retried separately, which multiplied the attempts and the total backoff.
func TestClient_RetriesAreNotNested(t *testing.T) {
	t.Parallel()

	const testMaxRetries = 3

	tests := []clientCall{
		startBackupCall(testIntegratedNamespace, testIntegratedStorage),
		startRestoreCall(testCmdBackupID, testIntegratedNamespace, testIntegratedStorage),
		abortBackupCall(testIntegratedJobID),
		abortRestoreCall(testIntegratedJobID, testIntegratedNamespace),
		openRestoreCall(testIntegratedJobID, testIntegratedNamespace),
		{
			name: "GetClusterStable",
			call: func(ctx context.Context, ic *Client) error {
				_, err := ic.GetClusterStable(ctx, testIntegratedNamespace)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var attempts int

			mockNodeGetter := mocks.NewMockNodeGetter(t)
			mockNodeGetter.EXPECT().GetRandomNode().RunAndReturn(func() (*a.Node, a.Error) {
				attempts++

				return nil, a.ErrInvalidParam
			})
			// GetClusterStable reads the node list before asking for the principal.
			// A single inactive node keeps it going.
			mockNodeGetter.EXPECT().GetNodes().Return([]*a.Node{{}}).Maybe()

			ic := newClient(mockNodeGetter, testInfoPolicy, models.NewRetryPolicy(0, 1, testMaxRetries))

			err := tt.call(t.Context(), ic)

			require.ErrorIs(t, err, errclass.ErrAerospike)
			require.Equal(t, testMaxRetries, attempts)
		})
	}
}

// TestClient_IntegratedBackupNotSupported checks that the integrated backup methods
// fail on an older server before any node is asked: the mocked cluster has no
// expectations, so any request to it fails the test.
func TestClient_IntegratedBackupNotSupported(t *testing.T) {
	t.Parallel()

	tests := integratedBackupClientCalls(testIntegratedJobID, testCmdBackupID, testIntegratedNamespace,
		testIntegratedStorage)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
			ic.cmds = newInfoCommands(testVersionBeforeIntegratedBackup())

			err := tt.call(t.Context(), ic)

			require.ErrorIs(t, err, errCommandNotSupported)
			require.ErrorIs(t, err, errclass.ErrUnsupported)
		})
	}
}

// TestClient_IntegratedBackupMissingParams checks that the integrated backup
// methods reject empty required parameters before any node is asked, so the
// error is not retried: the mocked cluster has no expectations, so any request
// to it fails the test.
func TestClient_IntegratedBackupMissingParams(t *testing.T) {
	t.Parallel()

	tests := integratedBackupClientCalls("", "", "", "")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			err := tt.call(t.Context(), ic)

			require.ErrorIs(t, err, errMissingCmdParam)
			require.ErrorIs(t, err, errclass.ErrInvalidConfig)
		})
	}
}

// Shared by the restore status tests below.
const (
	testRestoreJobID          = testCmdJobID
	testRestoreNamespace      = "source-ns1"
	testRestoreOtherNamespace = "source-ns2"
	testRestoreStatusCmd      = "restore-status:job-id=" + testRestoreJobID
	testBackupStatusCmd       = "backup-status:job-id=" + testRestoreJobID
	testNodeA                 = "BB9020011AC4202"
	testNodeB                 = "BB9030011AC4202"
	testNodeC                 = "BB9040011AC4202"

	// testRestoreJobOnNode starts the error of a node that does not know the
	// restore job; the name of the node follows.
	testRestoreJobOnNode = "restore job " + testRestoreJobID + " on node "
	// testNoRestoreJob is the answer of a node that does not know the restore job.
	testNoRestoreJob = errCmdRespPrefix + ":2:no restore job " + testRestoreJobID
	// testStatusUnavailable is the note of a start error whose status lookup failed.
	testStatusUnavailable = "status unavailable"
	// testNoBackupJob is the answer of a node that does not know the backup job.
	testNoBackupJob = errCmdRespPrefix + ":2:no backup job " + testRestoreJobID
)

// statusNodeAnswer describes the answers of a mocked node to the restore
// commands. An inactive node is never asked, and the restore-clear-storage
// command is expected only when clearResp is set.
type statusNodeAnswer struct {
	statusErr  a.Error
	name       string
	statusResp string
	clearResp  string
	active     bool
}

// newStatusNodeMocks returns a mocked node for every answer. A node is asked
// for its status with statusCmd and is sent clearCmd: a command that is not
// expected fails the test.
func newStatusNodeMocks(t *testing.T, answers []statusNodeAnswer, statusCmd, clearCmd string,
) []*mocks.MockstatusNode {
	t.Helper()

	nodes := make([]*mocks.MockstatusNode, 0, len(answers))

	for _, n := range answers {
		m := mocks.NewMockstatusNode(t)
		m.EXPECT().IsActive().Return(n.active).Once()
		m.EXPECT().GetName().Return(n.name).Maybe()

		if n.active {
			m.EXPECT().RequestInfo(testInfoPolicy, []string{statusCmd}).
				Return(map[string]string{statusCmd: n.statusResp}, n.statusErr).Once()
		}

		if n.clearResp != "" {
			m.EXPECT().RequestInfo(testInfoPolicy, []string{clearCmd}).
				Return(map[string]string{clearCmd: n.clearResp}, nil).Once()
		}

		nodes = append(nodes, m)
	}

	return nodes
}

// TestClearRestoreStorage checks that the clear is sent only to the nodes whose
// row of the job requires one, so that a retry never clears a node twice, and
// that a failing node does not keep the others from being cleared.
func TestClearRestoreStorage(t *testing.T) {
	t.Parallel()

	const (
		testNodeD    = "BB9050011AC4202"
		testAccepted = "ok"
		clearCmd     = "restore-clear-storage:namespace=" + testRestoreNamespace + ";job-id=" + testRestoreJobID
		// testColdRow is the restore-status row of a cold job being prepared.
		testColdRow = "job-id=%s:ns=%s:kind=cold:state=ACTIVE:ns-state=PREPARING:clear=%s"
		// testFuzzyRow is the restore-status row of a fuzzy job, which has no clear field.
		testFuzzyRow = "job-id=%s:ns=%s:kind=fuzzy:state=ACTIVE:ns-state=NONE"
	)

	coldRow := func(ns string, clearState infomodels.ClearState) string {
		return fmt.Sprintf(testColdRow, testRestoreJobID, ns, clearState)
	}

	tests := []struct {
		wantErrIs    error
		wantErrNotIs error
		name         string
		wantErrText  string
		nodes        []statusNodeAnswer
	}{
		{
			name: "only nodes that require a clear are cleared",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  testAccepted,
				},
				{
					name:       testNodeB,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateClearing),
				},
				{
					name:       testNodeC,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateNone),
				},
				{name: testNodeD},
			},
		},
		{
			name: "fuzzy job needs no clear",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: fmt.Sprintf(testFuzzyRow, testRestoreJobID, testRestoreNamespace),
				},
			},
		},
		{
			name: "stub row is skipped",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  testAccepted,
				},
				{
					name:   testNodeB,
					active: true,
					// Even a stub row that reports a clear is not sent one: it is
					// not the row of a namespace waiting for a clear.
					statusResp: coldRow("(none)", infomodels.ClearStateRequired),
				},
			},
		},
		{
			name: "refused clear does not stop the other nodes",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  errCmdRespPrefix + ":22:restore-clear-storage: rejected - set sindex-gc-period",
				},
				{
					name:       testNodeB,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  testAccepted,
				},
			},
			wantErrIs:   errclass.ErrAerospike,
			wantErrText: "failed to clear restore storage on node " + testNodeA,
		},
		{
			name: "unavailable status does not stop the other nodes",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusErr: a.ErrNetwork},
				{
					name:       testNodeB,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  testAccepted,
				},
			},
			wantErrIs:    errclass.ErrAerospike,
			wantErrNotIs: ErrNotFound,
			wantErrText:  testNodeA,
		},
		{
			name: "node knows no such job",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: testNoRestoreJob,
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreJobOnNode + testNodeA,
		},
		{
			name: "job unknown on one node does not stop the clear of the others",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: testNoRestoreJob,
				},
				{
					name:       testNodeB,
					active:     true,
					statusResp: coldRow(testRestoreNamespace, infomodels.ClearStateRequired),
					clearResp:  testAccepted,
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreJobOnNode + testNodeA,
		},
		{
			name: "job of another namespace",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: coldRow(testRestoreOtherNamespace, infomodels.ClearStateRequired),
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreNamespace,
		},
		{
			name:      "no active nodes",
			nodes:     []statusNodeAnswer{{name: testNodeA}},
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodes := newStatusNodeMocks(t, tt.nodes, testRestoreStatusCmd, clearCmd)

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			err := clearRestoreStorage(ic, nodes, testRestoreStatusCmd, clearCmd, testRestoreNamespace, testRestoreJobID)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, tt.wantErrText)

				if tt.wantErrNotIs != nil {
					require.NotErrorIs(t, err, tt.wantErrNotIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestClient_RestoreStatusInvalidNamespace checks that the methods that read
// restore-status filter its rows by a namespace validated before any node is
// asked, although restore-status does not send it: an empty namespace used to
// be reported as a job that was not found. The mocked cluster has no
// expectations, so any request to it fails the test.
func TestClient_RestoreStatusInvalidNamespace(t *testing.T) {
	t.Parallel()

	calls := []struct {
		call func(ctx context.Context, ic *Client, namespace string) error
		name string
	}{
		{
			name: testMethodGetRestoreStatus,
			call: func(ctx context.Context, ic *Client, namespace string) error {
				_, err := ic.GetRestoreStatus(ctx, namespace, testIntegratedJobID)

				return err
			},
		},
		{
			name: testMethodClearRestoreStorage,
			call: func(ctx context.Context, ic *Client, namespace string) error {
				return ic.ClearRestoreStorage(ctx, namespace, testIntegratedJobID)
			},
		},
	}

	tests := []struct {
		wantErrIs     error
		name          string
		giveNamespace string
	}{
		{
			name:      "empty namespace",
			wantErrIs: errMissingCmdParam,
		},
		{
			name:          "namespace with pipe",
			giveNamespace: testPipeValue,
			wantErrIs:     errInvalidCmdParam,
		},
	}

	for _, c := range calls {
		for _, tt := range tests {
			t.Run(c.name+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

				err := c.call(t.Context(), ic, tt.giveNamespace)

				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorIs(t, err, errclass.ErrInvalidConfig)
				require.NotErrorIs(t, err, ErrNotFound)
			})
		}
	}
}

// TestClient_sendStartBackup checks the error plumbing of the backup start. A
// start that failed must either be confirmed by the state of its own job id or
// be reported with its original cause: the cause used to be replaced by the
// status lookup error, or lost entirely when that lookup succeeded.
func TestClient_sendStartBackup(t *testing.T) {
	t.Parallel()

	const (
		testJobID     = testCmdJobID
		testNamespace = "source-ns1"
		testStartCmd  = "backup:namespace=" + testNamespace + ";job-id=" + testJobID
		testAccepted  = "ok"
		statusCmd     = testBackupStatusCmd
		testRejected  = errCmdRespPrefix + ":26:backup job " + testJobID +
			": this job-id is in use - poll backup-status, or pick another job-id"
		testTimedOut = errCmdRespPrefix + ":9:backup job " + testJobID +
			": started, but not acknowledged by every node within 5000 ms - poll backup-status"
		testUnconfirmed = errCmdRespPrefix + ":1:backup job " + testJobID +
			": start could not be confirmed - check backup-status before retrying"
	)

	runningStatus := fmt.Sprintf("job-id=%s:ns=%s:state=INCR_SCAN_ACTIVE", testJobID, testNamespace)
	failedStatus := fmt.Sprintf("job-id=%s:ns=%s:state=FAILED", testJobID, testNamespace)

	tests := []struct {
		startErr         a.Error
		statusErr        a.Error
		wantErrIs        error
		name             string
		startResp        string
		statusResp       string
		wantErrText      string
		wantErrNotText   string
		unansweredBefore bool
	}{
		{
			name:      "start accepted",
			startResp: testAccepted,
		},
		{
			name:       "start failed but the job is running",
			startErr:   a.ErrNetTimeout,
			statusResp: runningStatus,
		},
		{
			name:       "start failed and the job already failed on the server",
			startErr:   a.ErrNetTimeout,
			statusResp: failedStatus,
		},
		{
			name:      "start failed and the server holds no state for the job",
			startErr:  a.ErrNetTimeout,
			wantErrIs: a.ErrNetTimeout,
		},
		{
			name:        "start failed and the status is unavailable",
			startErr:    a.ErrNetTimeout,
			statusErr:   a.ErrNetwork,
			wantErrIs:   a.ErrNetTimeout,
			wantErrText: testStatusUnavailable,
		},
		{
			name:           "start failed and the node does not know the job",
			startErr:       a.ErrNetTimeout,
			statusResp:     testNoBackupJob,
			wantErrIs:      a.ErrNetTimeout,
			wantErrNotText: testStatusUnavailable,
		},
		{
			name:       "start response reports an error and the status is not read",
			startResp:  testRejected,
			statusResp: runningStatus,
			wantErrIs:  errclass.ErrAerospike,
		},
		{
			name:             "start response reports an error after an unanswered attempt",
			startResp:        testRejected,
			statusResp:       runningStatus,
			unansweredBefore: true,
		},
		{
			name:       "start not acknowledged in time but the job is running",
			startResp:  testTimedOut,
			statusResp: runningStatus,
		},
		{
			name:       "start not confirmed but the job is running",
			startResp:  testUnconfirmed,
			statusResp: runningStatus,
		},
		{
			name:       "start not acknowledged in time and the node does not know the job",
			startResp:  testTimedOut,
			statusResp: testNoBackupJob,
			wantErrIs:  errclass.ErrAerospike,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := mocks.NewMockinfoGetter(t)
			node.EXPECT().RequestInfo(testInfoPolicy, []string{testStartCmd}).
				Return(map[string]string{testStartCmd: tt.startResp}, tt.startErr).Once()
			// Registered for the job id command only: a lookup by namespace, as the
			// code used to do, has no matching expectation and fails the test.
			node.EXPECT().RequestInfo(testInfoPolicy, []string{statusCmd}).
				Return(map[string]string{statusCmd: tt.statusResp}, tt.statusErr).Maybe()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
			attempts := startAttempts{unanswered: tt.unansweredBefore}

			err := ic.sendStartBackup(node, testStartCmd, statusCmd, &attempts)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, tt.wantErrText)

				if tt.wantErrNotText != "" {
					assert.NotContains(t, err.Error(), tt.wantErrNotText)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestClient_sendStartRestore checks the error plumbing of the restore start.
// See TestClient_sendStartBackup for the cause that used to be lost.
func TestClient_sendStartRestore(t *testing.T) {
	t.Parallel()

	const (
		testJobID       = testCmdJobID
		testNamespace   = "source-ns1"
		testStartCmd    = "restore:namespace=" + testNamespace + ";job-id=" + testJobID
		testAccepted    = "ok"
		statusCmd       = testRestoreStatusCmd
		testRejected    = errCmdRespPrefix + ":22:restore: job-id=" + testJobID + " was used before - use a new job-id"
		testTimedOut    = errCmdRespPrefix + ":9:restore: composite publish timed out"
		testUnconfirmed = errCmdRespPrefix + ":1:restore: job-id=" + testJobID +
			": start could not be confirmed - check restore-status before retrying"
		// testColdStatus is the restore-status row of a cold job.
		testColdStatus = "job-id=%s:ns=%s:kind=cold:state=%s:restore-worker-threads=8:ns-state=%s"
	)

	restoringStatus := fmt.Sprintf(testColdStatus, testJobID, testNamespace,
		infomodels.RestoreStateActive, infomodels.NamespaceStateRestoring)
	preparingStatus := fmt.Sprintf(testColdStatus, testJobID, testNamespace,
		infomodels.RestoreStateActive, infomodels.NamespaceStatePreparing)
	failedStatus := fmt.Sprintf(testColdStatus, testJobID, testNamespace,
		infomodels.RestoreStateFailed, infomodels.NamespaceStateFailed)
	fuzzyStatus := fmt.Sprintf("job-id=%s:ns=%s:kind=fuzzy:state=%s", testJobID, testNamespace,
		infomodels.RestoreStateActive)

	tests := []struct {
		startErr         a.Error
		statusErr        a.Error
		wantErrIs        error
		name             string
		startResp        string
		statusResp       string
		wantErrText      string
		wantErrNotText   string
		unansweredBefore bool
	}{
		{
			name:      "start accepted",
			startResp: testAccepted,
		},
		{
			name:       "start failed but the restore is running",
			startErr:   a.ErrNetTimeout,
			statusResp: restoringStatus,
		},
		{
			name:       "start failed but the cold job is preparing",
			startErr:   a.ErrNetTimeout,
			statusResp: preparingStatus,
		},
		{
			name:       "start failed but the fuzzy job is running",
			startErr:   a.ErrNetTimeout,
			statusResp: fuzzyStatus,
		},
		{
			name:       "start failed and the job already failed on the server",
			startErr:   a.ErrNetTimeout,
			statusResp: failedStatus,
		},
		{
			name:      "start failed and the server holds no state for the job",
			startErr:  a.ErrNetTimeout,
			wantErrIs: a.ErrNetTimeout,
		},
		{
			name:        "start failed and the status is unavailable",
			startErr:    a.ErrNetTimeout,
			statusErr:   a.ErrNetwork,
			wantErrIs:   a.ErrNetTimeout,
			wantErrText: testStatusUnavailable,
		},
		{
			name:           "start failed and the node does not know the job",
			startErr:       a.ErrNetTimeout,
			statusResp:     testNoRestoreJob,
			wantErrIs:      a.ErrNetTimeout,
			wantErrNotText: testStatusUnavailable,
		},
		{
			name:       "start response reports an error and the status is not read",
			startResp:  testRejected,
			statusResp: restoringStatus,
			wantErrIs:  errclass.ErrAerospike,
		},
		{
			name:             "start response reports an error after an unanswered attempt",
			startResp:        testRejected,
			statusResp:       restoringStatus,
			unansweredBefore: true,
		},
		{
			name:       "start not acknowledged in time but the job is running",
			startResp:  testTimedOut,
			statusResp: restoringStatus,
		},
		{
			name:       "start not confirmed but the job is running",
			startResp:  testUnconfirmed,
			statusResp: restoringStatus,
		},
		{
			name:       "start not acknowledged in time and the node does not know the job",
			startResp:  testTimedOut,
			statusResp: testNoRestoreJob,
			wantErrIs:  errclass.ErrAerospike,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := mocks.NewMockinfoGetter(t)
			node.EXPECT().RequestInfo(testInfoPolicy, []string{testStartCmd}).
				Return(map[string]string{testStartCmd: tt.startResp}, tt.startErr).Once()
			// Registered for the job id command only: any other status lookup has
			// no matching expectation and fails the test.
			node.EXPECT().RequestInfo(testInfoPolicy, []string{statusCmd}).
				Return(map[string]string{statusCmd: tt.statusResp}, tt.statusErr).Maybe()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
			attempts := startAttempts{unanswered: tt.unansweredBefore}

			err := ic.sendStartRestore(node, testStartCmd, statusCmd, &attempts)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, tt.wantErrText)

				if tt.wantErrNotText != "" {
					assert.NotContains(t, err.Error(), tt.wantErrNotText)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

// Test_startAttempts_record checks that an attempt with an unknown outcome, one
// that went unanswered or that the server could not confirm, keeps the job
// possibly accepted for every later attempt, and that any other error answered
// by the server alone never does.
func Test_startAttempts_record(t *testing.T) {
	t.Parallel()

	answered := &cmdRespError{code: 4, err: errclass.ErrAerospike}
	unanswered := fmt.Errorf("%w: %w", errclass.ErrAerospike, a.ErrNetTimeout)
	timedOut := &cmdRespError{code: cmdRespCodeTimeout, err: errclass.ErrAerospike}
	unconfirmed := &cmdRespError{code: cmdRespCodeUnknown, err: errclass.ErrAerospike}

	tests := []struct {
		name string
		give []error
		want []bool
	}{
		{name: "answered only", give: []error{answered, answered}, want: []bool{false, false}},
		{name: "unanswered first", give: []error{unanswered, answered}, want: []bool{true, true}},
		{name: "unanswered later", give: []error{answered, unanswered, answered}, want: []bool{false, true, true}},
		{name: "not acknowledged in time", give: []error{timedOut, answered}, want: []bool{true, true}},
		{name: "not confirmed", give: []error{answered, unconfirmed}, want: []bool{false, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var attempts startAttempts

			got := make([]bool, 0, len(tt.give))
			for _, err := range tt.give {
				got = append(got, attempts.record(err))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

// TestStartWithRetry checks a start whose first attempt timed out after the
// server accepted the job, run through the retry loop of the start commands:
// the retry is refused by the server because the job id is in use, and the
// state of the job, shared through the attempts of the call, confirms the
// start.
func TestStartWithRetry(t *testing.T) {
	t.Parallel()

	const (
		testBackupStartCmd  = "backup:job-id=" + testRestoreJobID
		testRestoreStartCmd = "restore:job-id=" + testRestoreJobID
		// testBackupInUse and testRestoreUsed are the replies of the server to
		// a job id it already holds a job for.
		testBackupInUse = errCmdRespPrefix + ":22:backup: job-id=" + testRestoreJobID +
			" is in use or was used before - use a new job-id"
		testRestoreUsed = errCmdRespPrefix + ":22:restore: job-id=" + testRestoreJobID +
			" was used before - use a new job-id"
	)

	tests := []struct {
		send       func(ic *Client, node infoGetter, attempts *startAttempts) error
		name       string
		startCmd   string
		statusCmd  string
		notFound   string
		rejected   string
		statusResp string
	}{
		{
			name:       "backup",
			startCmd:   testBackupStartCmd,
			statusCmd:  testBackupStatusCmd,
			notFound:   testNoBackupJob,
			rejected:   testBackupInUse,
			statusResp: "job-id=" + testRestoreJobID + ":ns=" + testRestoreNamespace + ":state=BASE_SCAN_ACTIVE",
			send: func(ic *Client, node infoGetter, attempts *startAttempts) error {
				return ic.sendStartBackup(node, testBackupStartCmd, testBackupStatusCmd, attempts)
			},
		},
		{
			name:      "restore",
			startCmd:  testRestoreStartCmd,
			statusCmd: testRestoreStatusCmd,
			notFound:  testNoRestoreJob,
			rejected:  testRestoreUsed,
			statusResp: "job-id=" + testRestoreJobID + ":ns=" + testRestoreNamespace +
				":kind=cold:state=ACTIVE:ns-state=PREPARING",
			send: func(ic *Client, node infoGetter, attempts *startAttempts) error {
				return ic.sendStartRestore(node, testRestoreStartCmd, testRestoreStatusCmd, attempts)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := mocks.NewMockinfoGetter(t)
			node.EXPECT().RequestInfo(testInfoPolicy, []string{tt.startCmd}).
				Return(nil, a.ErrNetTimeout).Once()
			node.EXPECT().RequestInfo(testInfoPolicy, []string{tt.statusCmd}).
				Return(map[string]string{tt.statusCmd: tt.notFound}, nil).Once()
			node.EXPECT().RequestInfo(testInfoPolicy, []string{tt.startCmd}).
				Return(map[string]string{tt.startCmd: tt.rejected}, nil).Once()
			node.EXPECT().RequestInfo(testInfoPolicy, []string{tt.statusCmd}).
				Return(map[string]string{tt.statusCmd: tt.statusResp}, nil).Once()

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())
			policy := models.NewRetryPolicy(time.Millisecond, 1, 2)
			principal := func() (infoGetter, error) { return node, nil }

			err := startWithRetry(t.Context(), policy, principal, func(n infoGetter, attempts *startAttempts) error {
				return tt.send(ic, n, attempts)
			})

			require.NoError(t, err)
		})
	}
}

// TestGetRestoreStatus checks how the restore status rows of the nodes are
// collected: the inactive nodes are skipped, the rows of another namespace are
// not taken for the job, and the not found answer of a node names the job and
// the node.
func TestGetRestoreStatus(t *testing.T) {
	t.Parallel()

	const (
		// testStatusRow is the restore-status row of a cold job.
		testStatusRow = "job-id=%s:ns=%s:kind=cold:state=%s:ns-state=%s:ttl-extended=%d"
		// testNoName is the namespace of a stub row.
		testNoName = "(none)"
	)

	statusRow := func(ns string, state infomodels.RestoreState, nsState infomodels.NamespaceState,
		ttlExtended int,
	) string {
		return fmt.Sprintf(testStatusRow, testRestoreJobID, ns, state, nsState, ttlExtended)
	}

	tests := []struct {
		name         string
		nodes        []statusNodeAnswer
		want         *infomodels.ResponseRestoreState
		wantErrIs    error
		wantErrNotIs error
		wantErrText  string
	}{
		{
			name: "rows of the active nodes are merged",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 10),
				},
				{
					name:   testNodeB,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStatePreparing, 20),
				},
				{name: testNodeC},
			},
			want: &infomodels.ResponseRestoreState{
				JobID:       testRestoreJobID,
				Namespace:   testRestoreNamespace,
				Kind:        infomodels.RestoreKindCold,
				State:       infomodels.RestoreStateActive,
				NsState:     infomodels.NamespaceStatePreparing,
				TTLExtended: 30,
			},
		},
		{
			name: "stub row with no namespace is merged",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testNoName, infomodels.RestoreStateFailed,
						infomodels.NamespaceStateStub, 0),
				},
				{
					name:   testNodeB,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateFailed,
						infomodels.NamespaceStateReady, 0),
				},
			},
			want: &infomodels.ResponseRestoreState{
				JobID:     testRestoreJobID,
				Namespace: testRestoreNamespace,
				Kind:      infomodels.RestoreKindCold,
				State:     infomodels.RestoreStateFailed,
				NsState:   infomodels.NamespaceStateReady,
			},
		},
		{
			name: "stub row keeps a live job from looking complete",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateComplete,
						infomodels.NamespaceStateOpening, 0),
				},
				{
					name:   testNodeB,
					active: true,
					statusResp: statusRow(testNoName, infomodels.RestoreStateFailed,
						infomodels.NamespaceStateStub, 0),
				},
			},
			want: &infomodels.ResponseRestoreState{
				JobID:     testRestoreJobID,
				Namespace: testRestoreNamespace,
				Kind:      infomodels.RestoreKindCold,
				State:     infomodels.RestoreStateFailed,
				NsState:   infomodels.NamespaceStateOpening,
			},
		},
		{
			name: "job of another namespace",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreOtherNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 0),
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreNamespace,
		},
		{
			name: "job of another namespace on one node only",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 0),
				},
				{
					name:   testNodeB,
					active: true,
					statusResp: statusRow(testRestoreOtherNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 0),
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreNamespace,
		},
		{
			name: "node knows no such job",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 0),
				},
				{
					name:       testNodeB,
					active:     true,
					statusResp: testNoRestoreJob,
				},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreJobOnNode + testNodeB,
		},
		{
			name: "node that reports no row of the job",
			nodes: []statusNodeAnswer{
				{
					name:   testNodeA,
					active: true,
					statusResp: statusRow(testRestoreNamespace, infomodels.RestoreStateActive,
						infomodels.NamespaceStateReady, 0),
				},
				{name: testNodeB, active: true},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testRestoreJobOnNode + testNodeB,
		},
		{
			name: "error other than not found stops the merge",
			nodes: []statusNodeAnswer{
				{
					name:       testNodeA,
					active:     true,
					statusResp: errCmdRespPrefix + ":20:unknown namespace " + testRestoreNamespace,
				},
			},
			wantErrIs:    errclass.ErrAerospike,
			wantErrNotIs: ErrNotFound,
			wantErrText:  testNodeA,
		},
		{
			name: "node request failed",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusErr: a.ErrNetwork},
			},
			wantErrIs:    errclass.ErrAerospike,
			wantErrNotIs: ErrNotFound,
			wantErrText:  testNodeA,
		},
		{
			name:      "no active nodes",
			nodes:     []statusNodeAnswer{{name: testNodeA}},
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// No answer sets clearResp: getRestoreStatus must never clear a node.
			nodes := newStatusNodeMocks(t, tt.nodes, testRestoreStatusCmd, "")

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			got, err := getRestoreStatus(ic, nodes, testRestoreStatusCmd, testRestoreNamespace, testRestoreJobID)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, tt.wantErrText)

				if tt.wantErrNotIs != nil {
					require.NotErrorIs(t, err, tt.wantErrNotIs)
				}

				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGetBackupStatus checks how the backup status rows of the nodes are
// collected: the inactive nodes are skipped, and the job is not found when any
// active node does not know it.
func TestGetBackupStatus(t *testing.T) {
	t.Parallel()

	const (
		testBackupJobID     = testRestoreJobID
		testBackupNamespace = testRestoreNamespace
		testBackupJobOnNode = "backup job " + testBackupJobID + " on node "
		// testBackupRow is the backup-status row of a running job.
		testBackupRow = "job-id=" + testBackupJobID + ":ns=" + testBackupNamespace +
			":state=BASE_SCAN_ACTIVE:recs-base=%d"
	)

	tests := []struct {
		name         string
		nodes        []statusNodeAnswer
		want         *infomodels.ResponseBackupState
		wantErrIs    error
		wantErrNotIs error
		wantErrText  string
	}{
		{
			name: "rows of the active nodes are merged",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusResp: fmt.Sprintf(testBackupRow, 10)},
				{name: testNodeB, active: true, statusResp: fmt.Sprintf(testBackupRow, 20)},
				{name: testNodeC},
			},
			want: &infomodels.ResponseBackupState{
				JobID:     testBackupJobID,
				Namespace: testBackupNamespace,
				State:     infomodels.BackupStateBaseScanActive,
				RecsBase:  30,
			},
		},
		{
			name: "node that does not know the job",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusResp: fmt.Sprintf(testBackupRow, 10)},
				{name: testNodeB, active: true, statusResp: testNoBackupJob},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testBackupJobOnNode + testNodeB,
		},
		{
			name: "node that reports no row of the job",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusResp: fmt.Sprintf(testBackupRow, 10)},
				{name: testNodeB, active: true},
			},
			wantErrIs:   ErrNotFound,
			wantErrText: testBackupJobOnNode + testNodeB,
		},
		{
			name: "node request failed",
			nodes: []statusNodeAnswer{
				{name: testNodeA, active: true, statusErr: a.ErrNetwork},
			},
			wantErrIs:    errclass.ErrAerospike,
			wantErrNotIs: ErrNotFound,
			wantErrText:  testNodeA,
		},
		{
			name:      "no active nodes",
			nodes:     []statusNodeAnswer{{name: testNodeA}},
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nodes := newStatusNodeMocks(t, tt.nodes, testBackupStatusCmd, "")

			ic := newClient(mocks.NewMockNodeGetter(t), testInfoPolicy, models.NewDefaultRetryPolicy())

			got, err := getBackupStatus(ic, nodes, testBackupStatusCmd, testBackupJobID)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				require.ErrorContains(t, err, tt.wantErrText)

				if tt.wantErrNotIs != nil {
					require.NotErrorIs(t, err, tt.wantErrNotIs)
				}

				assert.Nil(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
