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
	"errors"
	"io"
	"log/slog"
	"testing"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/mocks"
	"github.com/aerospike/backup-go/models"
	pipemocks "github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type bufferWriteCloser struct {
	*bytes.Buffer
}

func (bwc *bufferWriteCloser) Close() error {
	return nil // no-op
}

func newBufferWriteCloser(buf *bytes.Buffer) io.WriteCloser {
	return &bufferWriteCloser{Buffer: buf}
}

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
	mockEncoder.EXPECT().WriteToken(recToken, mock.Anything).RunAndReturn(func(_ *models.Token, w io.Writer) (int, error) {
		return w.Write([]byte("encoded rec "))
	})
	mockEncoder.EXPECT().WriteToken(SIndexToken, mock.Anything).RunAndReturn(func(_ *models.Token, w io.Writer) (int, error) {
		return w.Write([]byte("encoded sindex "))
	})
	mockEncoder.EXPECT().WriteToken(UDFToken, mock.Anything).RunAndReturn(func(_ *models.Token, w io.Writer) (int, error) {
		return w.Write([]byte("encoded udf "))
	})
	mockEncoder.EXPECT().WriteToken(invalidToken, mock.Anything).Return(0, errors.New("error"))

	b := bytes.Buffer{}
	dst := newBufferWriteCloser(&b)
	writer := newTokenWriter[*models.Token](mockEncoder, dst, slog.Default(), nil)
	require.NotNil(t, writer)

	_, err := writer.Write(recToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec ", b.String())

	_, err = writer.Write(SIndexToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec encoded sindex ", b.String())

	_, err = writer.Write(UDFToken)
	require.NoError(t, err)
	require.Equal(t, "encoded rec encoded sindex encoded udf ", b.String())

	_, err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	require.NotNil(t, err)
	require.Equal(t, "encoded rec encoded sindex encoded udf ", b.String())

	failRec := &models.Record{
		Record: &a.Record{},
	}
	failRecToken := models.NewRecordToken(failRec, 0, nil)
	mockEncoder.EXPECT().WriteToken(failRecToken, mock.Anything).Return(0, errors.New("error"))
	_, err = writer.Write(failRecToken)
	require.NotNil(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestTokenStatsWriter(t *testing.T) {
	t.Parallel()

	mockWriter := pipemocks.NewMockWriter[*models.Token](t)

	mockWriter.EXPECT().Write(models.NewRecordToken(&models.Record{}, 0, nil)).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(models.NewUDFToken(&models.UDF{}, 0)).Return(1, nil)
	mockWriter.EXPECT().Write(&models.Token{Type: models.TokenTypeInvalid}).Return(0, errors.New("error"))
	mockWriter.EXPECT().Close().Return(nil)

	mockStats := mocks.NewMockstatsSetterToken(t)
	mockStats.EXPECT().AddUDFs(uint32(1))
	mockStats.EXPECT().AddSIndexes(uint32(1))

	writer := newWriterWithTokenStats[*models.Token](mockWriter, mockStats, slog.Default())
	require.NotNil(t, writer)

	_, err := writer.Write(models.NewRecordToken(&models.Record{}, 0, nil))
	require.NoError(t, err)

	_, err = writer.Write(models.NewSIndexToken(&models.SIndex{}, 0))
	require.NoError(t, err)

	_, err = writer.Write(models.NewUDFToken(&models.UDF{}, 0))
	require.NoError(t, err)

	_, err = writer.Write(&models.Token{Type: models.TokenTypeInvalid})
	require.NotNil(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

func TestTokenStatsWriterWriterFailed(t *testing.T) {
	t.Parallel()

	mockWriter := pipemocks.NewMockWriter[*models.Token](t)

	mockWriter.EXPECT().Write(models.NewSIndexToken(&models.SIndex{}, 0)).Return(0, errors.New("error"))

	mockStats := mocks.NewMockstatsSetterToken(t)

	writer := newWriterWithTokenStats[*models.Token](mockWriter, mockStats, slog.Default())
	require.NotNil(t, writer)

	_, err := writer.Write(models.NewSIndexToken(&models.SIndex{}, 0))
	require.Error(t, err)
}

// BenchmarkTokenWriter_Write benchmarks the full tokenWriter.Write path
// which encodes a token and writes it to the output.
func BenchmarkTokenWriter_Write(b *testing.B) {
	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", false, false))
	output := newNoCloseWriter(io.Discard)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	writer := newTokenWriter[*models.Token](encoder, output, logger, nil)

	key, _ := a.NewKey("test", "test", "key1")
	record := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"bin1": "value1",
				"bin2": int64(123),
				"bin3": []byte{1, 2, 3, 5, 6, 7, 8, 9, 10},
			},
		},
	}
	token := &models.Token{
		Type:   models.TokenTypeRecord,
		Record: record,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := writer.Write(token)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTokenWriter_Write_LargeRecord benchmarks with a larger record payload.
func BenchmarkTokenWriter_Write_LargeRecord(b *testing.B) {
	encoder := asb.NewEncoder[*models.Token](asb.NewEncoderConfig("test", false, false))
	output := newNoCloseWriter(io.Discard)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	writer := newTokenWriter[*models.Token](encoder, output, logger, nil)

	// Create a larger record with more bins and data
	largeBytes := make([]byte, 1024)
	for i := range largeBytes {
		largeBytes[i] = byte(i % 256)
	}

	key, _ := a.NewKey("test", "test", "key1")
	record := &models.Record{
		Record: &a.Record{
			Key: key,
			Bins: a.BinMap{
				"bin1":  "value1",
				"bin2":  int64(123),
				"bin3":  largeBytes,
				"bin4":  "a longer string value that takes more space",
				"bin5":  int64(9876543210),
				"bin6":  float64(3.14159265359),
				"bin7":  true,
				"bin8":  a.GeoJSONValue(`{"type": "Point", "coordinates": [100.0, 0.0]}`),
				"bin9":  []byte("another blob of binary data"),
				"bin10": "yet another string",
			},
			Generation: 42,
		},
		VoidTime: 1234567890,
	}
	token := &models.Token{
		Type:   models.TokenTypeRecord,
		Record: record,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := writer.Write(token)
		if err != nil {
			b.Fatal(err)
		}
	}
}
