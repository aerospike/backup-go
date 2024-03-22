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

package logging

import "log/slog"

func WithClient(logger *slog.Logger, id string) *slog.Logger {
	return logger.With("id", id).WithGroup("client")
}

type HandlerType string

const (
	HandlerTypeUnknown          HandlerType = "unknown"
	HandlerTypeBackup           HandlerType = "backup"
	HandlerTypeRestore          HandlerType = "restore"
	HandlerTypeBackupDirectory  HandlerType = "backup_directory"
	HandlerTypeRestoreDirectory HandlerType = "restore_directory"
)

func WithHandler(logger *slog.Logger, id string, handlerType HandlerType) *slog.Logger {
	return logger.With("id", id, "type", handlerType).WithGroup("handler")
}
