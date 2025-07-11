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

package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aerospike/backup-go/cmd/asbackup/cmd"
)

var (
	appVersion = cmd.VersionDev
	commitHash = cmd.VersionDev
)

func main() {
	// Initializing context with cancel for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, os.Kill)

	go func() {
		sig := <-sigChan
		log.Printf("stopping asbackup: %v\n", sig)
		cancel()
	}()

	// Return c to log errors properly.
	rootCmd, c := cmd.NewCmd(appVersion, commitHash)
	rootCmd.SilenceErrors = true

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		c.Logger.Error("failed to execute", slog.Any("error", err))
		os.Exit(1)
	}
}
