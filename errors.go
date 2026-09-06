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

package backup

import "github.com/aerospike/backup-go/errclass"

// Error classes reported by this library.
//
// Errors a caller can act on are wrapped with exactly one of them, so a
// failure can be classified without matching on error strings:
//
//	handler, err := client.Backup(ctx, config, writer, nil)
//	switch {
//	case errors.Is(err, backup.ErrInvalidConfig):
//		// bad input, retrying will not help
//	case errors.Is(err, backup.ErrStorage):
//		// infrastructure failure, worth retrying
//	}
//
// The class is only a marker: the wrapped message keeps all the details, and
// the underlying error stays reachable through errors.Is and errors.As.
//
// These are aliases of the values declared in
// [github.com/aerospike/backup-go/errclass], which the internal packages use
// directly. The values are identical, so errors.Is matches either spelling.
//
// Which class is attached is part of the library contract; the message text
// is not, and may change between releases.
var (
	// ErrInvalidConfig marks a configuration or an argument rejected by
	// validation. See [errclass.ErrInvalidConfig].
	ErrInvalidConfig = errclass.ErrInvalidConfig

	// ErrNotFound marks a backup, a directory, a file or an object that does
	// not exist. See [errclass.ErrNotFound].
	ErrNotFound = errclass.ErrNotFound

	// ErrStorage marks a failure reported by a storage backend.
	// See [errclass.ErrStorage].
	ErrStorage = errclass.ErrStorage

	// ErrCorruptData marks backup content that cannot be trusted.
	// See [errclass.ErrCorruptData].
	ErrCorruptData = errclass.ErrCorruptData

	// ErrUnsupported marks a mode, a format or a feature this version of the
	// library cannot handle. See [errclass.ErrUnsupported].
	ErrUnsupported = errclass.ErrUnsupported

	// ErrAerospike marks a failure reported by the Aerospike cluster or by the
	// Aerospike client. See [errclass.ErrAerospike].
	ErrAerospike = errclass.ErrAerospike

	// ErrSecretAgent marks a failure while talking to the Aerospike Secret
	// Agent. See [errclass.ErrSecretAgent].
	ErrSecretAgent = errclass.ErrSecretAgent
)
