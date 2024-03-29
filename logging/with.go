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
	return logger.WithGroup("handler").With("id", id, "type", handlerType)
}

type ReaderType string

const (
	ReaderTypeUnknown ReaderType = "unknown"
	ReaderTypeToken   ReaderType = "token"
	ReaderTypeRecord  ReaderType = "record"
	ReaderTypeSIndex  ReaderType = "sindex"
)

func WithReader(logger *slog.Logger, id string, readerType ReaderType) *slog.Logger {
	return logger.WithGroup("reader").With("id", id, "type", readerType)
}

type ProcessorType string

const (
	ProcessorTypeUnknown  ProcessorType = "unknown"
	ProcessorTypeTTL      ProcessorType = "token"
	ProcessorTypeVoidTime ProcessorType = "void_time"
)

func WithProcessor(logger *slog.Logger, id string, processorType ProcessorType) *slog.Logger {
	return logger.WithGroup("processor").With("id", id, "type", processorType)
}

type WriterType string

const (
	WriterTypeUnknown    WriterType = "unknown"
	WriterTypeTokenStats WriterType = "token_stats"
	WriterTypeToken      WriterType = "token"
	WriterTypeRestore    WriterType = "restore"
)

func WithWriter(logger *slog.Logger, id string, writerType WriterType) *slog.Logger {
	return logger.WithGroup("writer").With("id", id, "type", writerType)
}
