package backup

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	saClient "github.com/aerospike/backup-go/pkg/secret-agent"
)

const secretPrefix = "secrets:"

func getSecret(config *SecretAgentConfig, key string) (string, error) {
	// Getting resource and key.
	resource, secretKey, err := getResourceKey(key)
	if err != nil {
		return "", err
	}
	// Getting tls config.
	tlsConfig, err := getTlSConfig(config.CaFile)
	if err != nil {
		return "", err
	}

	// Parsing config values.
	address := *config.Address
	if config.Port != nil {
		address = fmt.Sprintf("%s:%d", *config.Address, *config.Port)
	}

	// Parsing timeout, if it is a nil, by default we set 1000 Millisecond
	timeout := 1000 * time.Millisecond
	if config.TimeoutMillisecond != nil {
		timeout = time.Duration(*config.TimeoutMillisecond) * time.Millisecond
	}

	// Parsing isBase64 param.
	var isBase64 bool
	if config.IsBase64 != nil {
		isBase64 = *config.IsBase64
	}

	// Initializing client.
	client, err := saClient.NewClient(
		*config.ConnectionType,
		address,
		timeout,
		isBase64,
		tlsConfig,
	)
	if err != nil {
		return "", fmt.Errorf("failed to initialize secret agent client: %w", err)
	}

	result, err := client.GetSecret(resource, secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to get secret from secret agent: %w", err)
	}

	return result, nil
}

func getResourceKey(key string) (resource, secretKey string, err error) {
	if !isSecret(key) {
		return "", "",
			fmt.Errorf("invalid secret key format, must be secrets:<resource>:<secret>")
	}

	keyArr := strings.Split(key, ":")
	if len(keyArr) != 3 {
		return "", "", fmt.Errorf("invalid secret key format")
	}
	// We believe that keyArr[0] == secretPrefix
	return keyArr[1], keyArr[2], nil
}

// getTlSConfig returns *tls.Config if caFile is set, or nil if caFile is not set.
func getTlSConfig(caFile *string) (*tls.Config, error) {
	if caFile == nil {
		return nil, nil
	}

	caCert, err := os.ReadFile(*caFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read ca file: %w", err)
	}

	caCertPool := x509.NewCertPool()

	ok := caCertPool.AppendCertsFromPEM(caCert)
	if !ok {
		return nil, fmt.Errorf("nothing to append to ca cert pool")
	}

	//nolint:gosec // we must support any tls configuration for legacy.
	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	return tlsConfig, nil
}

// isSecret checks if string is secret. e.g.: secrets:resource2:cacert
func isSecret(secret string) bool {
	return strings.HasPrefix(secret, secretPrefix)
}
