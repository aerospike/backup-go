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

package benchmark

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sort"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/io/local"
	"golang.org/x/sync/semaphore"
)

var backupClient = newBackupClient()

const runs = 10

func main() {
	folder := "backups_folder"

	backupGo := measureMedianTime(func() {
		runBackup(folder)
	})

	backupC := measureMedianTime(func() {
		_, _ = exec.
			Command("asbackup", "-U", "tester", "-P", "psw", "-n", "source-ns1", "-d", folder, "-r").
			CombinedOutput()
	})

	runRestore(folder)
	restoreGo := measureMedianTime(func() {
		runRestore(folder)
	})

	restoreC := measureMedianTime(func() {
		_, _ = exec.
			Command("asrestore", "-U", "tester", "-P", "psw", "-d", folder, "-g").
			CombinedOutput()
	})

	fmt.Printf("Median time after %d runs:\nBackup GO:\t%.2f\nBackup C:\t%.2f\nRestore GO:\t%.2f\nRestoreC:\t%.2f\n",
		runs, backupGo.Seconds(), backupC.Seconds(), restoreGo.Seconds(), restoreC.Seconds())
}

func runRestore(folder string) {
	ctx := context.Background()
	restoreCfg := backup.NewDefaultRestoreConfig()
	restoreCfg.Parallel = 20

	// For restore from single file use local.WithFile(fileName)
	reader, err := local.NewReader(
		local.WithDir(folder),
	)
	if err != nil {
		panic(err)
	}

	restoreHandler, err := backupClient.Restore(ctx, restoreCfg, reader)
	if err != nil {
		panic(err)
	}

	err = restoreHandler.Wait(ctx)
	if err != nil {
		log.Printf("Restore failed: %v", err)
	}

	// optionally check the stats of the restore job
	fmt.Printf("Restored %d records\n", restoreHandler.GetStats().GetReadRecords())
}

func runBackup(folder string) {
	writers, err := local.NewWriter(
		local.WithRemoveFiles(),
		local.WithDir(folder),
	)
	if err != nil {
		panic(err)
	}

	backupCfg := backup.NewDefaultBackupConfig()
	backupCfg.Namespace = "source-ns1"
	backupCfg.ParallelRead = 4
	backupCfg.ParallelWrite = 20
	ctx := context.Background()

	backupHandler, err := backupClient.Backup(ctx, backupCfg, writers)
	if err != nil {
		panic(err)
	}

	err = backupHandler.Wait(ctx)
	if err != nil {
		log.Printf("Backup failed: %v", err)
	}

	fmt.Printf("backed up %d records\n", backupHandler.GetStats().GetReadRecords())
}

func newBackupClient() *backup.Client {
	policy := aerospike.NewClientPolicy()
	policy.User = "tester"
	policy.Password = "psw"
	aerospikeClient, aerr := aerospike.NewClientWithPolicy(policy, "127.0.0.1", 3000)
	if aerr != nil {
		panic(aerr)
	}

	backupClient, err := backup.NewClient(aerospikeClient, backup.WithID("client_id"), backup.WithScanLimiter(semaphore.NewWeighted(20)))
	if err != nil {
		panic(err)
	}

	return backupClient
}

func measureMedianTime(fn func()) time.Duration {
	fn() // warm
	times := make([]time.Duration, runs)

	for i := 0; i < runs; i++ {
		start := time.Now()
		fn()
		times[i] = time.Since(start)
		time.Sleep(1 * time.Second)
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i] < times[j]
	})

	return times[runs/2]
}
