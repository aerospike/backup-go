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

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/flags"
	"github.com/aerospike/backup-go/cmd/internal/logging"
	"github.com/aerospike/backup-go/cmd/internal/restore"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
)

const VersionDev = "dev"

// Cmd represents the base command when called without any subcommands
type Cmd struct {
	// Version params.
	appVersion string
	commitHash string

	// Root flags
	flagsApp          *flags.App
	flagsAerospike    *asFlags.AerospikeFlags
	flagsClientPolicy *flags.ClientPolicy
	flagsCompression  *flags.Compression
	flagsEncryption   *flags.Encryption
	flagsSecretAgent  *flags.SecretAgent
	flagsAws          *flags.AwsS3
	flagsGcp          *flags.GcpStorage
	flagsAzure        *flags.AzureBlob

	// Restore flags.
	flagsRestore *flags.Restore
	flagsCommon  *flags.Common

	Logger *slog.Logger
}

func NewCmd(appVersion, commitHash string) (*cobra.Command, *Cmd) {
	c := &Cmd{
		appVersion: appVersion,
		commitHash: commitHash,

		flagsApp:          flags.NewApp(),
		flagsAerospike:    asFlags.NewDefaultAerospikeFlags(),
		flagsClientPolicy: flags.NewClientPolicy(),
		flagsCommon:       flags.NewCommon(flags.OperationRestore),
		flagsRestore:      flags.NewRestore(),
		flagsCompression:  flags.NewCompression(flags.OperationRestore),
		flagsEncryption:   flags.NewEncryption(flags.OperationRestore),
		flagsSecretAgent:  flags.NewSecretAgent(),
		flagsAws:          flags.NewAwsS3(flags.OperationRestore),
		flagsGcp:          flags.NewGcpStorage(flags.OperationRestore),
		flagsAzure:        flags.NewAzureBlob(flags.OperationRestore),
		// First init default logger.
		Logger: logging.NewDefaultLogger(),
	}

	rootCmd := &cobra.Command{
		Use:   "asrestore",
		Short: "Aerospike restore CLI tool",
		Long:  "Welcome to the Aerospike restore CLI tool!",
		RunE:  c.run,
	}

	// Disable sorting
	rootCmd.PersistentFlags().SortFlags = false
	rootCmd.SilenceUsage = true

	appFlagSet := c.flagsApp.NewFlagSet()
	aerospikeFlagSet := c.flagsAerospike.NewFlagSet(asFlags.DefaultWrapHelpString)
	clientPolicyFlagSet := c.flagsClientPolicy.NewFlagSet()
	commonFlagSet := c.flagsCommon.NewFlagSet()
	restoreFlagSet := c.flagsRestore.NewFlagSet()
	compressionFlagSet := c.flagsCompression.NewFlagSet()
	encryptionFlagSet := c.flagsEncryption.NewFlagSet()
	secretAgentFlagSet := c.flagsSecretAgent.NewFlagSet()
	awsFlagSet := c.flagsAws.NewFlagSet()
	gcpFlagSet := c.flagsGcp.NewFlagSet()
	azureFlagSet := c.flagsAzure.NewFlagSet()

	// App flags.
	rootCmd.PersistentFlags().AddFlagSet(appFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(aerospikeFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(clientPolicyFlagSet)

	rootCmd.Flags().AddFlagSet(commonFlagSet)
	rootCmd.Flags().AddFlagSet(restoreFlagSet)

	rootCmd.PersistentFlags().AddFlagSet(compressionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(encryptionFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(secretAgentFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(awsFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(gcpFlagSet)
	rootCmd.PersistentFlags().AddFlagSet(azureFlagSet)

	// Beautify help and usage.
	helpFunc := func() {
		fmt.Println("Welcome to the Aerospike restore CLI tool!")
		fmt.Println("-----------------------------------------")

		fmt.Println("The restore tool automatically identifies and " +
			"restores ASB and ASBX backup files found in the specified folder.")
		fmt.Println("You can set restore mode manually with --mode flag. " +
			"Flags that are incompatible with restore mode,")
		fmt.Println("are also incompatible in automatic mode (when mode is not set).")

		fmt.Println("\nUsage:")
		fmt.Println("  asrestore [flags]")

		// Print section: App Flags
		fmt.Println("\nGeneral Flags:")
		appFlagSet.PrintDefaults()

		// Print section: Common Flags
		fmt.Println("\nAerospike Client Flags:")
		aerospikeFlagSet.PrintDefaults()
		clientPolicyFlagSet.PrintDefaults()

		// Print section: Restore Flags
		fmt.Println("\nRestore Flags:")
		commonFlagSet.PrintDefaults()
		restoreFlagSet.PrintDefaults()

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
			"For S3 storage bucket name is mandatory, and is set with --s3-bucket-name flag.\n" +
			"So --directory path will only contain folder name.\n" +
			"--s3-endpoint-override is used in case you want to use minio, instead of AWS.\n" +
			"Any AWS parameter can be retrieved from Secret Agent.")
		awsFlagSet.PrintDefaults()

		// Print section: GCP Flags
		fmt.Println("\nGCP Flags:\n" +
			"For GCP storage bucket name is mandatory, and is set with --gcp-bucket-name flag.\n" +
			"So --directory path will only contain folder name.\n" +
			"Flag --gcp-endpoint-override is mandatory, as each storage account has different service address.\n" +
			"Any GCP parameter can be retrieved from Secret Agent.")
		gcpFlagSet.PrintDefaults()

		// Print section: Azure Flags
		fmt.Println("\nAzure Flags:\n" +
			"For Azure storage container name is mandatory, and is set with --azure-storage-container-name flag.\n" +
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

	return rootCmd, c
}

func (c *Cmd) run(cmd *cobra.Command, _ []string) error {
	// Show version.
	if c.flagsApp.Version {
		c.printVersion()

		return nil
	}

	// If no flags were passed, show help.
	if cmd.Flags().NFlag() == 0 {
		if err := cmd.Help(); err != nil {
			return fmt.Errorf("failed to load help: %w", err)
		}

		return nil
	}

	// Init logger.
	logger, err := logging.NewLogger(c.flagsApp.LogLevel, c.flagsApp.Verbose, c.flagsApp.LogJSON)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	// After initialization replace logger.
	c.Logger = logger

	// Init app.
	asrParams := &config.RestoreParams{
		App:          c.flagsApp.GetApp(),
		ClientConfig: c.flagsAerospike.NewAerospikeConfig(),
		ClientPolicy: c.flagsClientPolicy.GetClientPolicy(),
		Restore:      c.flagsRestore.GetRestore(),
		Common:       c.flagsCommon.GetCommon(),
		Compression:  c.flagsCompression.GetCompression(),
		Encryption:   c.flagsEncryption.GetEncryption(),
		SecretAgent:  c.flagsSecretAgent.GetSecretAgent(),
		AwsS3:        c.flagsAws.GetAwsS3(),
		GcpStorage:   c.flagsGcp.GetGcpStorage(),
		AzureBlob:    c.flagsAzure.GetAzureBlob(),
	}

	logMsg := "restore"
	if asrParams.Restore.ValidateOnly {
		logMsg = "validation"
	}

	asr, err := restore.NewService(cmd.Context(), asrParams, logger)
	if err != nil {
		return fmt.Errorf("%s initialization failed: %w", logMsg, err)
	}

	if err = asr.Run(cmd.Context()); err != nil {
		return fmt.Errorf("%s failed: %w", logMsg, err)
	}

	return nil
}

func (c *Cmd) printVersion() {
	version := c.appVersion
	if c.appVersion == VersionDev {
		version += " (" + c.commitHash + ")"
	}

	fmt.Printf("version: %s\n", version)
}
