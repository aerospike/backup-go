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

package aerospike

import (
	"fmt"
	"log/slog"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/models"
	"github.com/google/uuid"
)

type udfWriter struct {
	asc         dbWriter
	writePolicy *a.WritePolicy
	logger      *slog.Logger
}

// writeUDF writes a UDF to the Aerospike database.
func (rw udfWriter) writeUDF(udf *models.UDF) error {
	uuid, _ := uuid.NewRandom()
	fmt.Println("Write UDF TOKEN:", uuid)
	defer fmt.Println("UDF DONE TOKEN:", uuid)

	var UDFLang a.Language

	switch udf.UDFType {
	case models.UDFTypeLUA:
		UDFLang = a.LUA
	default:
		return fmt.Errorf("error registering UDF %s: invalid UDF language %b", udf.Name, udf.UDFType)
	}

	job, aerr := rw.asc.RegisterUDF(rw.writePolicy, udf.Content, udf.Name, UDFLang)
	if aerr != nil {
		return fmt.Errorf("error registering UDF %s: %w", udf.Name, aerr)
	}

	if job == nil {
		return fmt.Errorf("error registering UDF %s: job is nil", udf.Name)
	}

	errs := job.OnComplete()
	if errs == nil {
		return fmt.Errorf("error registering UDF %s: OnComplete returned nil channel", udf.Name)
	}

	err := <-errs
	if err != nil {
		return fmt.Errorf("error registering UDF %s: %w", udf.Name, err)
	}

	rw.logger.Info("registered UDF", slog.String("name", udf.Name))

	return nil
}
