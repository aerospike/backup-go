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

package backup

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	pipemocks "github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTokenWriter(t *testing.T) {
	t.Parallel()

	namespace := "test"
	set := ""

	key, aerr := a.NewKey(namespace, set, "key")
	if aerr != nil {
		panic(aerr)
	}

	expRecord := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"key0": "hi",
				"key1": 1,
			},
		},
	}
	recToken := models.NewRecordToken(expRecord, 0, nil)

	expUDF := &models.UDF{
		Name: "udf",
	}
	UDFToken := models.NewUDFToken(expUDF, 0)

	expSIndex := &models.SIndex{
		Name: "sindex",
	}
	SIndexToken := models.NewSIndexToken(expSIndex, 0)

	invalidToken := &models.Token{Type: models.TokenTypeInvalid}

	mockEncoder := mocks.NewMockEncoder[*models.Token](t)
	mockEncoder.EXPECT().EncodeToken(recToken).Return([]byte("encoded rec "), nil)
	mockEncoder.EXPECT().EncodeToken(SIndexToken).Return([]byte("encoded sindex "), nil)
	mockEncoder.EXPECT().EncodeToken(UDFToken).Return([]byte("encoded udf "), nil)
	mockEncoder.EXPECT().EncodeToken(invalidToken).Return(nil, errors.New("error"))

	dst := bytes.Buffer{}
	writer := newTokenWriter[*models.Token](mockEncoder, &dst, slog.Default(), nil)
	require.NotNil(t, writer)

	ctx := context.Background()

	_, err := writer.Write(ctx, recToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec ", dst.String())

	_, err = writer.Write(ctx, SIndexToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec encoded sindex ", dst.String())

	_, err = writer.Write(ctx, UDFToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec encoded sindex encoded udf ", dst.String())

	_, err = writer.Write(ctx, &models.Token{Type: models.TokenTypeInvalid})
	require.NotNil(t, err)
	require.Equal(t, "encoded rec encoded sindex encoded udf ", dst.String())

	failRec := &models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec, 0, nil)
	mockEncoder.EXPECT().EncodeToken(failRecToken).Return(nil, errors.New("error"))
	_, err = writer.Write(ctx, failRecToken)
	require.NotNil(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestTokenStatsWriter(t *testing.T) {
	t.Parallel()

	mockWriter := pipemocks.NewMockWriter[*models.Token](t)

	mockWriter.EXPECT().Write(mock.Anything, models.NewRecordToken(&models.Record{}, 0, nil)).Return(1, nil)
	mockWriter.EXPECT().Write(mock.Anything, models.NewSIndexToken(&models.SIndex{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(mock.Anything, models.NewUDFToken(&models.UDF{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(mock.Anything, &models.Token{Type: models.TokenTypeInvalid}).Return(0, errors.New("error"))
	mockWriter.EXPECT().Close().Return(nil)

	mockStats := mocks.NewMockstatsSetterToken(t)
	mockStats.EXPECT().AddUDFs(uint32(1))
	mockStats.EXPECT().AddSIndexes(uint32(1))

	writer := newWriterWithTokenStats[*models.Token](mockWriter, mockStats, slog.Default())
	require.NotNil(t, writer)

	ctx := context.Background()

	_, err := writer.Write(ctx, models.NewRecordToken(&models.Record{}, 0, nil))
	require.NoError(t, err)

	_, err = writer.Write(ctx, models.NewSIndexToken(&models.SIndex{}, 0))
	require.NoError(t, err)

	_, err = writer.Write(ctx, models.NewUDFToken(&models.UDF{}, 0))
	require.NoError(t, err)

	_, err = writer.Write(ctx, &models.Token{Type: models.TokenTypeInvalid})
	require.NotNil(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestTokenStatsWriterWriterFailed(t *testing.T) {
	t.Parallel()

	mockWriter := pipemocks.NewMockWriter[*models.Token](t)

	mockWriter.EXPECT().Write(mock.Anything, models.NewSIndexToken(&models.SIndex{}, 0)).Return(0, errors.New("error"))

	mockStats := mocks.NewMockstatsSetterToken(t)

	writer := newWriterWithTokenStats[*models.Token](mockWriter, mockStats, slog.Default())
	require.NotNil(t, writer)

	ctx := context.Background()

	_, err := writer.Write(ctx, models.NewSIndexToken(&models.SIndex{}, 0))
	require.Error(t, err)
}
