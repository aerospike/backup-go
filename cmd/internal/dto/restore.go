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

package dto

// Restore is used to map yaml config.
type Restore struct {
	App struct {
		Verbose  bool   `yaml:"verbose"`
		LogLevel string `yaml:"log-level"`
		LogJSON  bool   `yaml:"log-json"`
	} `yaml:"app"`
	Cluster struct {
		Seeds []struct {
			Host    string `yaml:"host"`
			TLSName string `yaml:"tls-name"`
			Port    int    `yaml:"port"`
		} `yaml:"seeds"`
		User               string `yaml:"user"`
		Password           string `yaml:"password"`
		Auth               string `yaml:"auth"`
		ClientTimeout      int    `yaml:"client-timeout"`
		ClientIdleTimeout  int    `yaml:"client-idle-timeout"`
		ClientLoginTimeout int    `yaml:"client-login-timeout"`
		TLS                struct {
			Enable          bool   `yaml:"enable"`
			Name            string `yaml:"name"`
			Protocols       string `yaml:"protocols"`
			CaFile          string `yaml:"ca-file"`
			CaPath          string `yaml:"ca-path"`
			CertFile        string `yaml:"cert-file"`
			KeyFile         string `yaml:"key-file"`
			KeyFilePassword string `yaml:"key-file-password"`
		} `yaml:"tls"`
	} `yaml:"cluster"`
	Restore struct {
		Directory          string  `yaml:"directory"`
		Namespace          string  `yaml:"namespace"`
		SetList            string  `yaml:"set-list"`
		BinList            string  `yaml:"bin-list"`
		Parallel           int     `yaml:"parallel"`
		NoRecords          bool    `yaml:"no-records"`
		NoIndexes          bool    `yaml:"no-indexes"`
		NoUDFs             bool    `yaml:"no-udfs"`
		RecordsPerSecond   int     `yaml:"records-per-second"`
		MaxRetries         int     `yaml:"max-retries"`
		TotalTimeout       int64   `yaml:"total-timeout"`
		SocketTimeout      int64   `yaml:"socket-timeout"`
		Nice               int     `yaml:"nice"`
		InputFile          string  `yaml:"input-file"`
		DirectoryList      string  `yaml:"directory-list"`
		ParentDirectory    string  `yaml:"parent-directory"`
		DisableBatchWrites bool    `yaml:"disable-batch-writes"`
		BatchSize          int     `yaml:"batch-size"`
		MaxAsyncBatches    int     `yaml:"max-async-batches"`
		WarmUp             int     `yaml:"warm-up"`
		ExtraTTL           int64   `yaml:"extra-ttl"`
		IgnoreRecordError  bool    `yaml:"ignore-record-error"`
		Uniq               bool    `yaml:"uniq"`
		Replace            bool    `yaml:"replace"`
		NoGeneration       bool    `yaml:"no-generation"`
		TimeOut            int64   `yaml:"timeout"`
		RetryBaseTimeout   int64   `yaml:"retry-base-timeout"`
		RetryMultiplier    float64 `yaml:"retry-multiplier"`
		RetryMaxRetries    uint    `yaml:"retry-max-retries"`
		Mode               string  `yaml:"mode"`
		ValidateOnly       bool    `yaml:"validate-only"`
	} `yaml:"restore"`
	Compression struct {
		Mode  string `yaml:"mode"`
		Level int    `yaml:"level"`
	} `yaml:"compression"`
	Encryption struct {
		Mode      string `yaml:"mode"`
		KeyFile   string `yaml:"key-file"`
		KeyEnv    string `yaml:"key-env"`
		KeySecret string `yaml:"key-secret"`
	} `yaml:"encryption"`
	SecretAgent struct {
		ConnectionType     string `yaml:"connection-type"`
		Address            string `yaml:"address"`
		Port               int    `yaml:"port"`
		TimeoutMillisecond int    `yaml:"timeout-millisecond"`
		CaFile             string `yaml:"ca-file"`
		IsBase64           bool   `yaml:"is-base64"`
	} `yaml:"secret-agent"`
	Aws struct {
		S3 struct {
			BucketName       string `yaml:"bucket-name"`
			Region           string `yaml:"region"`
			Profile          string `yaml:"profile"`
			EndpointOverride string `yaml:"endpoint-override"`
			AccessKeyID      string `yaml:"access-key-id"`
			SecretAccessKey  string `yaml:"secret-access-key"`
			StorageClass     string `yaml:"storage-class"`
			RetryMaxAttempts int    `yaml:"retry-max-attempts"`
			RetryMaxBackoff  int    `yaml:"retry-max-backoff"`
			RetryBackoff     int    `yaml:"retry-backoff"`
			ChunkSize        int    `yaml:"chunk-size"`
		} `yaml:"s3"`
	} `yaml:"aws"`
	Gcp struct {
		Storage struct {
			KeyFile                string `yaml:"key-file"`
			BucketName             string `yaml:"bucket-name"`
			EndpointOverride       string `yaml:"endpoint-override"`
			RetryMaxAttempts       int    `yaml:"retry-max-attempts"`
			RetryMaxBackoff        int    `yaml:"retry-max-backoff"`
			RetryInitBackoff       int    `yaml:"retry-init-backoff"`
			RetryBackoffMultiplier int    `yaml:"retry-backoff-multiplier"`
			ChunkSize              int    `yaml:"chunk-size"`
		} `yaml:"storage"`
	} `yaml:"gcp"`
	Azure struct {
		Blob struct {
			AccountName           string `yaml:"account-name"`
			AccountKey            string `yaml:"account-key"`
			TenantID              string `yaml:"tenant-id"`
			ClientID              string `yaml:"client-id"`
			ClientSecret          string `yaml:"client-secret"`
			EndpointOverride      string `yaml:"endpoint-override"`
			ContainerName         string `yaml:"container-name"`
			AccessTier            string `yaml:"access-tier"`
			RehydratePollDuration int    `yaml:"rehydrate-poll-duration"`
			RetryMaxAttempts      int    `yaml:"retry-max-attempts"`
			RetryTimeout          int    `yaml:"retry-timeout"`
			RetryDelay            int    `yaml:"retry-delay"`
			RetryMaxDelay         int    `yaml:"retry-max-delay"`
		} `yaml:"blob"`
	} `yaml:"azure"`
}
