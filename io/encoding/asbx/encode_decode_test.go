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

package asbx

import (
	"bytes"
	"encoding/base64"
	"io"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace  = "source-ns1"
	testSetName    = "set1"
	testDigestB64  = "/+Ptyjj06wW9zx0AnxOmq45xJzs="
	testPayloadB64 = "FhABEAAAAAAAAgAnjQAAAAAAAAUAAQAAAAsAc291cmNlLW5zMQAAABUE/+Ptyjj06wW9zx0AnxOmq45xJzsAAAAFAXNldDEAAAAKAgEAAAAAAAADCQAAAAkOAAAAbcndaZgAAAAUAgMAAWF6enp6enp6enp6enp6eno="
	testKeyString  = "source-ns1:::ff e3 ed ca 38 f4 eb 05 bd cf 1d 00 9f 13 a6 ab 8e 71 27 3b"
	testFileName   = "source-ns1_1.asbx"
	testFileNumber = 1
)

func testToken() (*models.ASBXToken, error) {
	digest, err := base64.StdEncoding.DecodeString(testDigestB64)
	if err != nil {
		return nil, err
	}

	payload, err := base64.StdEncoding.DecodeString(testPayloadB64)
	if err != nil {
		return nil, err
	}

	k, err := aerospike.NewKeyWithDigest(testNamespace, testSetName, "", digest)
	if err != nil {
		return nil, err
	}

	return models.NewASBXToken(k, payload), nil
}

func TestEncoder_Decoder(t *testing.T) {
	// Encode.
	content := make([]byte, 0)
	enc := NewEncoder[*models.ASBXToken](testNamespace)

	token, err := testToken()
	require.NoError(t, err)

	fileName := enc.GenerateFilename("", "")
	require.Equal(t, testFileName, fileName)

	h := enc.GetHeader()
	content = append(content, h...)

	et, err := enc.EncodeToken(token)
	require.NoError(t, err)
	content = append(content, et...)

	// Decode.
	reader := bytes.NewReader(content)
	dec, err := NewDecoder[*models.ASBXToken](reader)
	require.NoError(t, err)

	nt, err := dec.NextToken()
	require.NoError(t, err)

	require.Equal(t, testKeyString, nt.Key.String())
	payloadB64 := base64.StdEncoding.EncodeToString(nt.Payload)
	require.Equal(t, testPayloadB64, payloadB64)
}

func TestDecoder_ErrorHeader(t *testing.T) {
	content := make([]byte, 0)

	reader := bytes.NewReader(content)
	_, err := NewDecoder[*models.ASBXToken](reader)
	require.ErrorIs(t, err, io.EOF)
}

func TestDecoder_ErrorToken(t *testing.T) {
	enc := NewEncoder[*models.ASBXToken](testNamespace)

	fileName := enc.GenerateFilename("", "")
	require.Equal(t, testFileName, fileName)

	content := make([]byte, 0)
	h := enc.GetHeader()
	content = append(content, h...)

	reader := bytes.NewReader(content)
	dec, err := NewDecoder[*models.ASBXToken](reader)
	require.NoError(t, err)

	_, err = dec.NextToken()
	require.ErrorIs(t, err, io.EOF)
}

//
// func TestDecoder_ErrorFileNumber(t *testing.T) {
// 	enc := NewEncoder[*models.ASBXToken](testNamespace)
//
// 	content := make([]byte, 0)
// 	h := enc.GetHeader()
// 	content = append(content, h...)
//
// 	reader := bytes.NewReader(content)
// 	_, err := NewDecoder[*models.ASBXToken](reader)
// 	require.Equal(t, "file number mismatch got 1, want 0", err.Error())
// }
