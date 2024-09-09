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
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"

	"github.com/aerospike/backup-go/cmd/internal/app"
	"github.com/aerospike/backup-go/cmd/internal/flags"
	common "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
)

const devVersion = "dev"

var (
	appVersion = devVersion
	commitHash = "dev"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "asbackup",
	Short: "Aerospike backup CLI tool",
	Long:  "Welcome to the Aerospike backup CLI tool!",
	RunE:  run,
}

var (
	flagsCommon      = common.NewDefaultAerospikeFlags()
	flagsBackup      = flags.NewBackup()
	flagsCompression = flags.NewCompression()
	flagsEncryption  = flags.NewEncryption()
	flagsSecretAgent = flags.NewSecretAgent()
	flagsStorage     = flags.NewStorage()
	flagsAws         = flags.NewAwsS3()
	flagsGcp         = flags.NewGcpStorage()
	flagsAzure       = flags.NewAzureBlob()

	flagVersion bool
	flagVerbose bool
)

func init() {
	// Override -h flag
	rootCmd.PersistentFlags().BoolP("help", "Z", false, "Display help information")
	rootCmd.PersistentFlags().BoolVarP(&flagVersion, "version", "V",
		false, "Display version information")
	rootCmd.PersistentFlags().BoolVarP(&flagVerbose, "verbose", "v",
		false,
		"Enable more detailed logging.")
	// App flags.
	rootCmd.PersistentFlags().AddFlagSet(flagsCommon.NewFlagSet(func(str string) string { return str }))
	rootCmd.PersistentFlags().AddFlagSet(flagsBackup.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsCompression.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsEncryption.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsSecretAgent.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsStorage.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsAws.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsGcp.NewFlagSet())
	rootCmd.PersistentFlags().AddFlagSet(flagsAzure.NewFlagSet())
}

func run(cmd *cobra.Command, _ []string) error {
	// Show version.
	if flagVersion {
		printVersion()

		return nil
	}
	// If no flags were passed, show help.
	if cmd.Flags().NFlag() == 0 {
		if err := cmd.Help(); err != nil {
			return err
		}

		return nil
	}
	// Init logger.
	loggerOpt := &slog.HandlerOptions{}
	if flagVerbose {
		loggerOpt.Level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, loggerOpt))
	// Init app.
	asb, err := app.NewASBackup(
		cmd.Context(),
		flagsCommon.NewAerospikeConfig(),
		flagsBackup.GetBackup(),
		flagsCompression.GetCompression(),
		flagsEncryption.GetEncryption(),
		flagsSecretAgent.GetSecretAgent(),
		flagsStorage.GetStorage(),
		flagsAws.GetAwsS3(),
		flagsGcp.GetGcpStorage(),
		flagsAzure.GetAzureBlob(),
		logger)
	if err != nil {
		return err
	}
	// Run app.
	return asb.Run(cmd.Context())
}

func printVersion() {
	version := appVersion
	if appVersion == devVersion {
		version += "." + commitHash
	}

	fmt.Printf("version: %s\n", version)
}

func main() {
	// Initializing context with cancel for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		sig := <-sigChan
		log.Printf("stopping click-copier: %v\n", sig)
		cancel()
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
