package workers

import (
	parser "backuplib/marshal"
	"errors"
	"io"
	"log"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v7"
)

type BackupMarshaller interface {
	MarshalRecord(*a.Record) ([]byte, error)
}

type OutputOpener interface {
	Open() (io.WriteCloser, error)
	Resume() (io.WriteCloser, error)
}

type BackupJobConfig struct {
	Namespace      string
	Set            string
	Output         OutputOpener
	Marshaller     BackupMarshaller
	FirstPartition int
	NumPartitions  int
	First          bool
}

type backupStatus struct {
	partitionFilter  *a.PartitionFilter
	metadataFinished bool
	backupStarted    bool // if this is true, backup resumption is possible
	backupFinished   bool
	scanPolicy       *a.ScanPolicy
}

type BackupJob struct {
	config       *BackupJobConfig
	status       *backupStatus
	client       *a.Client
	workLock     *sync.Mutex
	writer       io.WriteCloser
	outputOpener OutputOpener
}

func NewBackupJob(cfg *BackupJobConfig, client *a.Client) (*BackupJob, error) {
	wc, err := cfg.Output.Open()
	if err != nil {
		return nil, err
	}

	job := &BackupJob{
		config:       cfg,
		client:       client,
		writer:       wc,
		outputOpener: cfg.Output,
		workLock:     &sync.Mutex{},
		status:       &backupStatus{},
	}

	return job, nil
}

func (j *BackupJob) lock() {
	j.workLock.Lock()
}

func (j *BackupJob) unlock() {
	j.workLock.Unlock()
}

func (j *BackupJob) Run() error {
	j.lock()
	defer j.unlock()

	err := runBackup(j)
	if err != nil {
		return err
	}

	return nil
}

func (j *BackupJob) Resume() error {
	j.lock()
	defer j.unlock()

	wc, err := j.outputOpener.Resume()
	if err != nil {
		return err
	}
	j.writer = wc

	if !j.status.backupStarted {
		return errors.New("record backup must have been started in order to resume")
	}

	if j.status.partitionFilter == nil {
		return errors.New("cannot resume a backup job with a nil partition filter")
	}

	err = runBackup(j)
	if err != nil {
		return err
	}

	return nil
}

// **** Helper Functions

func runBackup(j *BackupJob) error {
	// TODO do single shot work
	// if j.status.first {
	// }

	err := writeMetadata(j)
	if err != nil {
		return err
	}

	// the partition filer will not be nil if this job is being resumed
	if j.status.partitionFilter == nil {
		j.status.partitionFilter = a.NewPartitionFilterByRange(
			j.config.FirstPartition,
			j.config.NumPartitions,
		)
	}

	policy := a.NewScanPolicy()
	j.status.scanPolicy = policy

	err = backupRecords(j)
	if err != nil {
		return err
	}

	err = j.writer.Close()
	if err != nil {
		return err
	}

	j.status.backupFinished = true

	return nil
}

func writeMetadata(j *BackupJob) error {
	versionText := parser.GetVersionText()
	_, err := j.writer.Write(versionText)
	if err != nil {
		log.Println(err)
		return err
	}

	metaDataText := parser.GetNamespaceMetaText(j.config.Namespace)

	if j.config.First {
		firstMetaText := parser.GetFirstMetaText()
		metaDataText = append(metaDataText, firstMetaText...)
	}

	_, err = j.writer.Write(metaDataText)
	if err != nil {
		log.Println(err)
		return err
	}

	j.status.metadataFinished = true

	return nil
}

func backupRecords(j *BackupJob) error {
	recSet, err := j.client.ScanPartitions(
		j.status.scanPolicy,
		j.status.partitionFilter,
		j.config.Namespace,
		j.config.Set,
	)
	if err != nil {
		return err
	}

	j.status.backupStarted = true

	for rec := range recSet.Results() {
		if rec.Err != nil {
			log.Println(err)
			return rec.Err
		}

		text, err := j.config.Marshaller.MarshalRecord(rec.Record)
		if err != nil {
			log.Println(err)
			return err
		}

		_, err = j.writer.Write(text)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}
