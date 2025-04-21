package backup

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	a "github.com/aerospike/aerospike-client-go/v8"
	ioStorage "github.com/aerospike/backup-go/io/storage"
	"github.com/aerospike/backup-go/io/storage/local"
	"github.com/stretchr/testify/require"
)

func Test_Backup_pprof(t *testing.T) {
	t.Parallel()
	const setName = "mrtLoad"

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	aeroClientPolicy := a.NewClientPolicy()
	aeroClientPolicy.User = "tester"
	aeroClientPolicy.Password = "psw"
	aeroClientPolicy.Timeout = testTimeout
	asClient, aErr := a.NewClientWithPolicy(
		aeroClientPolicy,
		testASHost,
		testASPort,
	)
	require.NoError(t, aErr)
	defer asClient.Close()

	backupConfig := NewDefaultBackupConfig()
	backupConfig.SetList = []string{setName}
	backupConfig.NoIndexes = true
	backupConfig.Namespace = "source-ns1"

	directory := path.Join(t.TempDir(), fmt.Sprintf("%s_%d", setName, time.Now().UnixNano()))

	writer, err := local.NewWriter(
		ctx,
		ioStorage.WithRemoveFiles(),
		ioStorage.WithDir(directory),
	)
	require.NoError(t, err)

	backupClient, err := NewClient(asClient, WithID("test_client"))
	require.NoError(t, err)

	bh, err := backupClient.Backup(
		ctx,
		backupConfig,
		writer,
		nil,
	)
	require.NoError(t, err)

	err = bh.Wait(ctx)
	require.NoError(t, err)
}
