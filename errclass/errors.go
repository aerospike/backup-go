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

package errclass

import "errors"

// Error classes reported by the library.
//
// Errors a caller can act on are wrapped with exactly one of them, so a
// failure can be classified without matching on error strings:
//
//	if errors.Is(err, errclass.ErrInvalidConfig) {
//		// bad input, retrying will not help
//	}
//
// The class is only a marker: the wrapped message keeps all the details, and
// the underlying error (an SDK error, a syscall error, an Aerospike client
// error) stays reachable through errors.Is and errors.As.
//
// The root backup package re-exports these under shorter names
// (backup.ErrInvalidConfig and so on) — the values are identical, so
// errors.Is matches either spelling.
//
// A class is attached once, where the error is created or where an error from
// another library first enters this one; the layers above only add context
// with %w, so the class survives all the way to the caller. Errors that carry
// no class are internal invariants a caller cannot act on differently.
//
// Which class is attached is part of the library contract; the message text
// is not, and may change between releases.
var (
	// ErrInvalidConfig marks a configuration or an argument rejected by
	// validation. The operation was never started; retrying is pointless
	// until the caller fixes the input.
	ErrInvalidConfig = errors.New("invalid config")

	// ErrNotFound marks a backup, a directory, a file or an object that does
	// not exist. It is also returned when a listing yields nothing where at
	// least one entry was required.
	ErrNotFound = errors.New("not found")

	// ErrStorage marks a failure reported by a storage backend: the local
	// file system, AWS S3, GCP Storage or Azure Blob Storage. These are
	// infrastructure failures and are usually worth retrying.
	ErrStorage = errors.New("storage error")

	// ErrCorruptData marks backup content that cannot be trusted: a malformed
	// token, a truncated or oversized record, a segment that fails its
	// integrity checks. Retrying the same source will not help.
	ErrCorruptData = errors.New("corrupt data")

	// ErrUnsupported marks a mode, a format or a feature that this version of
	// the library cannot handle: an unknown compression or encryption mode, an
	// unsupported token or connection type, a backup produced by an
	// incompatible tool.
	ErrUnsupported = errors.New("unsupported")

	// ErrAerospike marks a failure reported by the Aerospike cluster or by the
	// Aerospike client: info commands, scans, batch writes, UDF and secondary
	// index operations. The client's own error is preserved in the chain, so
	// errors.As with the client error types still works.
	ErrAerospike = errors.New("aerospike error")

	// ErrSecretAgent marks a failure while talking to the Aerospike Secret
	// Agent: the agent is unreachable, the exchange timed out, or the agent
	// answered with an error. Config values that could not be resolved because
	// of it surface as this class, not as ErrInvalidConfig: the configuration
	// itself may well be correct.
	ErrSecretAgent = errors.New("secret agent error")
)
