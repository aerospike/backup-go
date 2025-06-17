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

package xdr

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/aerospike/backup-go/cmd/internal/backup"
	"github.com/aerospike/backup-go/cmd/internal/config"
	"github.com/aerospike/backup-go/cmd/internal/flags"
	"github.com/aerospike/backup-go/cmd/internal/logging"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Cmd represents XDR sub command.
type Cmd struct {
	// Flags fromm root.
	flagsApp          *flags.App
	flagsAerospike    *asFlags.AerospikeFlags
	flagsClientPolicy *flags.ClientPolicy
	flagsCompression  *flags.Compression
	flagsEncryption   *flags.Encryption
	flagsSecretAgent  *flags.SecretAgent
	flagsAws          *flags.AwsS3
	flagsGcp          *flags.GcpStorage
	flagsAzure        *flags.AzureBlob

	// Xdr flags
	flagsBackupXDR *flags.BackupXDR
}

// NewCmd return initialized xdr command.
func NewCmd(
	flagsApp *flags.App,
	flagsAerospike *asFlags.AerospikeFlags,
	flagsClientPolicy *flags.ClientPolicy,
	flagsCompression *flags.Compression,
	flagsEncryption *flags.Encryption,
	flagsSecretAgent *flags.SecretAgent,
	flagsAws *flags.AwsS3,
	flagsGcp *flags.GcpStorage,
	flagsAzure *flags.AzureBlob,
) *cobra.Command {
	c := &Cmd{
		flagsApp:          flagsApp,
		flagsAerospike:    flagsAerospike,
		flagsClientPolicy: flagsClientPolicy,
		flagsCompression:  flagsCompression,
		flagsEncryption:   flagsEncryption,
		flagsSecretAgent:  flagsSecretAgent,
		flagsAws:          flagsAws,
		flagsGcp:          flagsGcp,
		flagsAzure:        flagsAzure,
	}

	xdrCmd := &cobra.Command{
		Use:   "xdr",
		Short: "Aerospike XDR backup CLI tool",
		RunE:  c.run,
	}

	c.flagsBackupXDR = flags.NewBackupXDR()
	backupXDRFlagSet := c.flagsBackupXDR.NewFlagSet()

	// XDR flags.
	xdrCmd.Flags().AddFlagSet(backupXDRFlagSet)

	// Beautify help and usage.
	helpFunc := newHelpFunction(backupXDRFlagSet)

	xdrCmd.SetUsageFunc(func(_ *cobra.Command) error {
		helpFunc()
		return nil
	})

	xdrCmd.SetHelpFunc(func(_ *cobra.Command, _ []string) {
		helpFunc()
	})

	return xdrCmd
}

func (c *Cmd) run(cmd *cobra.Command, _ []string) error {
	// If no flags were passed, show help.
	if cmd.Flags().NFlag() == 0 {
		if err := cmd.Help(); err != nil {
			log.Println(err)

			return err
		}

		return nil
	}

	// Init logger.
	logger, err := logging.NewLogger(c.flagsApp.LogLevel, c.flagsApp.Verbose, c.flagsApp.LogJSON)
	if err != nil {
		log.Println(err)

		return err
	}

	// Init app.
	asbParams, err := config.NewBackupParams(
		c.flagsApp.GetApp(),
		c.flagsAerospike.NewAerospikeConfig(),
		c.flagsClientPolicy.GetClientPolicy(),
		nil,
		c.flagsBackupXDR.GetBackupXDR(),
		c.flagsCompression.GetCompression(),
		c.flagsEncryption.GetEncryption(),
		c.flagsSecretAgent.GetSecretAgent(),
		c.flagsAws.GetAwsS3(),
		c.flagsGcp.GetGcpStorage(),
		c.flagsAzure.GetAzureBlob(),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize app: %w", err)
	}

	asb, err := backup.NewService(cmd.Context(), asbParams, logger)
	if err != nil {
		logger.Error("backup initialization failed", slog.Any("error", err))

		return err
	}

	if err = asb.Run(cmd.Context()); err != nil {
		logger.Error("backup failed", slog.Any("error", err))

		return err
	}

	return nil
}

func newHelpFunction(backupXDRFlagSet *pflag.FlagSet) func() {
	return func() {
		fmt.Println("Welcome to the Aerospike XDR backup CLI tool!")
		fmt.Println("-----------------------------------------")
		fmt.Println("\nUsage:")
		fmt.Println("  asbackup xdr [flags]")
		// Print section: XDR Flags
		fmt.Println("\nXDR Backup Flags:")
		fmt.Println("This sections replace Backup Flags section in main documentation." +
			"\nAll other flags are valid for XDR backup.")
		backupXDRFlagSet.PrintDefaults()
	}
}
