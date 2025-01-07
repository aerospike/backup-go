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
	"log/slog"
	"os"

	"github.com/aerospike/backup-go/cmd/internal/app"
	"github.com/aerospike/backup-go/cmd/internal/flags"
	asFlags "github.com/aerospike/tools-common-go/flags"
	"github.com/spf13/cobra"
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
	flagsRestoreXDR *flags.RestoreXDR
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

	c.flagsRestoreXDR = flags.NewRestoreXDR()
	restoreXDRFlagSet := c.flagsRestoreXDR.NewFlagSet()

	// XDR flags.
	xdrCmd.Flags().AddFlagSet(restoreXDRFlagSet)

	// Beautify help and usage.
	helpFunc := func() {
		fmt.Println("Welcome to the Aerospike XDR restore CLI tool!")
		fmt.Println("-----------------------------------------")
		fmt.Println("\nUsage:")
		fmt.Println("  asrestore xdr [flags]")
		// Print section: XDR Flags
		fmt.Println("\nXDR Restore Flags:")
		fmt.Println("This sections replace Restore Flags section in main documentation." +
			"\nAll other flags are valid for XDR backup.")
		restoreXDRFlagSet.PrintDefaults()
	}

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
			return err
		}

		return nil
	}

	// Init logger.
	loggerOpt := &slog.HandlerOptions{}
	if c.flagsApp.Verbose {
		loggerOpt.Level = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, loggerOpt))

	// Init app.
	asbParams := &app.ASRestoreParams{
		ClientConfig:     c.flagsAerospike.NewAerospikeConfig(),
		ClientPolicy:     c.flagsClientPolicy.GetClientPolicy(),
		RestoreXDRParams: c.flagsRestoreXDR.GetRestoreXDR(),
		Compression:      c.flagsCompression.GetCompression(),
		Encryption:       c.flagsEncryption.GetEncryption(),
		SecretAgent:      c.flagsSecretAgent.GetSecretAgent(),
		AwsS3:            c.flagsAws.GetAwsS3(),
		GcpStorage:       c.flagsGcp.GetGcpStorage(),
		AzureBlob:        c.flagsAzure.GetAzureBlob(),
	}

	asb, err := app.NewASRestore(cmd.Context(), asbParams, logger)
	if err != nil {
		return err
	}

	return asb.Run(cmd.Context())
}
