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
	"fmt"
	"log/slog"
	"runtime/debug"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/segmentio/asm/base64"
)

func handlePanic(errors chan<- error, logger *slog.Logger) {
	if r := recover(); r != nil {
		var err error

		panicMsg := "a backup operation has panicked:"
		if _, ok := r.(error); ok {
			err = fmt.Errorf(panicMsg+" caused by this error: \"%w\"\n", r.(error))
		} else {
			err = fmt.Errorf(panicMsg+" caused by: \"%v\"\n", r)
		}

		err = fmt.Errorf("%w, with stacktrace: \"%s\"", err, debug.Stack())
		logger.Debug("job failed", "error", err)

		errors <- err
	}
}

func doWork(errors chan<- error, logger *slog.Logger, work func() error) {
	// NOTE: order is important here
	// if we close the errors chan before we handle the panic,
	// the panic handler will attempt to send on a closed channel
	defer close(errors)
	defer handlePanic(errors, logger)

	logger.Debug("job starting")

	err := work()
	if err != nil {
		logger.Debug("job failed", "error", err)
		errors <- err

		return
	}

	logger.Debug("job done")
}

func splitNodes(nodes []*a.Node, numWorkers int) ([][]*a.Node, error) {
	if numWorkers < 1 {
		return nil, fmt.Errorf("numWorkers is less than 1, cannot split nodes")
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("number of nodes is less than 1, cannot split nodes")
	}

	result := make([][]*a.Node, numWorkers)

	for i, node := range nodes {
		workerIndex := i % numWorkers
		result[workerIndex] = append(result[workerIndex], node)
	}

	return result, nil
}

func newKeyByDigest(namespace, digest string) (*a.Key, error) {
	digestBytes, err := base64.StdEncoding.DecodeString(digest)
	if err != nil {
		return nil, fmt.Errorf("failed to decode after-digest: %w", err)
	}

	key, err := a.NewKeyWithDigest(namespace, "", "", digestBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to init key from digest: %w", err)
	}

	return key, nil
}
