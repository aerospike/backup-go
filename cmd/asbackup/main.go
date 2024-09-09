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
	flagsApp         = flags.NewApp()
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
	// Disable sorting
	rootCmd.PersistentFlags().SortFlags = false

	appFlagSet := flagsApp.NewFlagSet()
	commonFlagSet := flagsCommon.NewFlagSet(func(str string) string { return str })
	backupFlagSet := flagsBackup.NewFlagSet()
	compressionFlagSet := flagsCompression.NewFlagSet()
	encryptionFlagSet := flagsEncryption.NewFlagSet()
	secretAgentFlagSet := flagsSecretAgent.NewFlagSet()
	storageFlagSet := flagsStorage.NewFlagSet()
	awsFlagSet := flagsAws.NewFlagSet()
	gcpFlagSet := flagsGcp.NewFlagSet()
	azureFlagSet := flagsAzure.NewFlagSet()

	// App flags.
	rootCmd.PersistentFlags().AddFlagSet(appFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(commonFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(backupFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(storageFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(azureFlagSet)

	// Beautify help.
	rootCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		fmt.Println("Welcome to the Aerospike backup CLI tool!")
		fmt.Println("\nUsage:")
		fmt.Println("  asbackup [flags]")

		// Print section: App Flags
		fmt.Println("\nGeneral Flags:")
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println("\nAerospike Client Flags:")
		commonFlagSet.PrintDefaults()

		// Print section: Backup Flags
		fmt.Println("\nBackup Flags:")
		backupFlagSet.PrintDefaults()

		// Print section: Compression Flags
		fmt.Println("\nCompression Flags:")
		compressionFlagSet.PrintDefaults()

		// Print section: Encryption Flags
		fmt.Println("\nEncryption Flags:")
		encryptionFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println("\nSecret Agent Flags:")
		secretAgentFlagSet.PrintDefaults()

		// Print section: Storage Flags
		fmt.Println("\nStorage Flags:")
		storageFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println("\nAWS Flags:")
		awsFlagSet.PrintDefaults()

		// Print section: GCP Flags
		fmt.Println("\nGCP Flags:")
		gcpFlagSet.PrintDefaults()

		// Print section: Azure Flags
		fmt.Println("\nAzure Flags:")
		azureFlagSet.PrintDefaults()
	})
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
		log.Printf("stopping asbackup: %v\n", sig)
		cancel()
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
