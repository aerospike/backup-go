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
	asFlags "github.com/aerospike/tools-common-go/flags"
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
	flagsApp          = flags.NewApp()
	flagsAerospike    = asFlags.NewDefaultAerospikeFlags()
	flagsClientPolicy = flags.NewClientPolicy()
	flagsCommon       = flags.NewCommon(flags.OperationBackup)
	flagsBackup       = flags.NewBackup()
	flagsCompression  = flags.NewCompression(flags.OperationBackup)
	flagsEncryption   = flags.NewEncryption(flags.OperationBackup)
	flagsSecretAgent  = flags.NewSecretAgent()
	flagsAws          = flags.NewAwsS3()
	flagsGcp          = flags.NewGcpStorage()
	flagsAzure        = flags.NewAzureBlob()
)

func init() {
	// Disable sorting
	rootCmd.PersistentFlags().SortFlags = false
	rootCmd.SilenceUsage = true

	appFlagSet := flagsApp.NewFlagSet()
	aerospikeFlagSet := flagsAerospike.NewFlagSet(func(str string) string { return str })
	clientPolicyFlagSet := flagsClientPolicy.NewFlagSet()
	commonFlagSet := flagsCommon.NewFlagSet()
	backupFlagSet := flagsBackup.NewFlagSet()
	compressionFlagSet := flagsCompression.NewFlagSet()
	encryptionFlagSet := flagsEncryption.NewFlagSet()
	secretAgentFlagSet := flagsSecretAgent.NewFlagSet()
	awsFlagSet := flagsAws.NewFlagSet()
	gcpFlagSet := flagsGcp.NewFlagSet()
	azureFlagSet := flagsAzure.NewFlagSet()

	// App flags.
	rootCmd.PersistentFlags().AddFlagSet(appFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(commonFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(backupFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(azureFlagSet)

	// Beautify help and usage.
	helpFunc := func() {
		fmt.Println("Welcome to the Aerospike backup CLI tool!")
		fmt.Println("-----------------------------------------")
		fmt.Println("\nUsage:")
		fmt.Println("  asbackup [flags]")

		// Print section: App Flags
		fmt.Println("\nGeneral Flags:")
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println("\nAerospike Client Flags:")
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Backup Flags
		fmt.Println("\nBackup Flags:")
		commonFlagSet.PrintDefaults()
		backupFlagSet.PrintDefaults()

		// Print section: Compression Flags
		fmt.Println("\nCompression Flags:")
		compressionFlagSet.PrintDefaults()

		// Print section: Encryption Flags
		fmt.Println("\nEncryption Flags:")
		encryptionFlagSet.PrintDefaults()

		// Print section: Secret Agent Flags
		fmt.Println("\nSecret Agent Flags:\n" +
			"Options pertaining to the Aerospike Secret Agent.\n" +
			"See documentation here: https://aerospike.com/docs/tools/secret-agent.\n" +
			"Both asbackup and asrestore support getting all the cloud config parameters from the Aerospike Secret Agent.\n" +
			"To use a secret as an option, use this format: 'secrets:<resource_name>:<secret_name>' \n" +
			"Example: asbackup --azure-account-name secret:resource1:azaccount")
		secretAgentFlagSet.PrintDefaults()

		// Print section: AWS Flags
		fmt.Println("\nAWS Flags:\n" +
			"For S3 storage, bucket name is mandatory, and is set with --s3-bucket-name flag.\n" +
			"So --directory path will only contain folder name.\n" +
			"--s3-endpoint-override is used in case you want to use minio, instead of AWS.\n" +
			"Any AWS parameter can be retrieved from Secret Agent.")
		awsFlagSet.PrintDefaults()

		// Print section: GCP Flags
		fmt.Println("\nGCP Flags:\n" +
			"For GCP storage, bucket name is mandatory, and is set with --gcp-bucket-name flag.\n" +
			"So --directory path will only contain folder name.\n" +
			"Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.\n" +
			"Any GCP parameter can be retrieved from Secret Agent.")
		gcpFlagSet.PrintDefaults()

		// Print section: Azure Flags
		fmt.Println("\nAzure Flags:\n" +
			"For Azure storage, container name is mandatory, and is set with --azure-storage-container-name flag.\n" +
			"So --directory path will only contain folder name.\n" +
			"Flag --azure-endpoint is optional, and is used for tests with Azurit or any other Azure emulator.\n" +
			"For authentication you can use --azure-account-name and --azure-account-key, or \n" +
			"--azure-tenant-id, --azure-client-id and azure-client-secret.\n" +
			"Any Azure parameter can be retrieved from Secret Agent.")
		azureFlagSet.PrintDefaults()
	}

	rootCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})
	rootCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})
}

func run(cmd *cobra.Command, _ []string) error {
	// Show version.
	if flagsApp.Version {
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
	if flagsApp.Verbose {
		loggerOpt.Level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, loggerOpt))
	// Init app.
	asb, err := app.NewASBackup(
		cmd.Context(),
		flagsAerospike.NewAerospikeConfig(),
		flagsClientPolicy.GetClientPolicy(),
		flagsBackup.GetBackup(),
		flagsCommon.GetCommon(),
		flagsCompression.GetCompression(),
		flagsEncryption.GetEncryption(),
		flagsSecretAgent.GetSecretAgent(),
		flagsAws.GetAwsS3(),
		flagsGcp.GetGcpStorage(),
		flagsAzure.GetAzureBlob(),
		logger)
	if err != nil {
		return err
	}

	return asb.Run(cmd.Context())
}

func printVersion() {
	version := appVersion
	if appVersion == devVersion {
		version += " (" + commitHash + ")"
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
