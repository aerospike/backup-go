package backup

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// Encryption modes
const (
	EncryptNone   = "NONE"
	EncryptAES128 = "AES128"
	EncryptAES256 = "AES256"

	pemTemplate = "-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----"
)

// EncryptionPolicy contains backup encryption information.
// @Description EncryptionPolicy contains backup encryption information.
type EncryptionPolicy struct {
	// The path to the file containing the encryption key.
	KeyFile *string `yaml:"key-file,omitempty" json:"key-file,omitempty"`
	// The name of the environment variable containing the encryption key.
	KeyEnv *string `yaml:"key-env,omitempty" json:"key-env,omitempty"`
	// The secret keyword in Aerospike Secret Agent containing the encryption key.
	KeySecret *string `yaml:"key-secret,omitempty" json:"key-secret,omitempty"`
	// The encryption mode to be used (NONE, AES128, AES256)
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty" default:"NONE" enums:"NONE,AES128,AES256"`
}

// Validate validates the encryption policy.
func (p *EncryptionPolicy) Validate() error {
	if p == nil {
		return nil
	}

	if p.Mode != EncryptNone && p.Mode != EncryptAES128 && p.Mode != EncryptAES256 {
		return fmt.Errorf("invalid encryption mode: %s", p.Mode)
	}

	if p.KeyFile == nil && p.KeyEnv == nil && p.KeySecret == nil {
		return errors.New("encryption key location not specified")
	}

	// Only one parameter allowed to be set.
	if (p.KeyFile != nil && p.KeyEnv != nil) ||
		(p.KeyFile != nil && p.KeySecret != nil) ||
		(p.KeyEnv != nil && p.KeySecret != nil) {
		return fmt.Errorf("only one encryption key source may be specified")
	}

	return nil
}

func (p *EncryptionPolicy) ReadPrivateKey(agent *SecretAgentConfig) ([]byte, error) {
	var (
		pemData []byte
		err     error
	)

	switch {
	case p.KeyFile != nil:
		pemData, err = p.readPemFromFile()
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from file: %w", err)
		}
	case p.KeyEnv != nil:
		pemData, err = p.readPemFromEnv()
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from ENV: %w", err)
		}
	case p.KeySecret != nil:
		pemData, err = p.readPemFromSecret(agent)
		if err != nil {
			return nil, fmt.Errorf("unable to read PEM from secret agent: %w", err)
		}
	}

	// Decode the PEM file
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	key := privateKey.(*rsa.PrivateKey)
	// Originally asbackup converts the key to the PKCS1 format
	decodedKey := x509.MarshalPKCS1PrivateKey(key)

	sum256 := sha256.Sum256(decodedKey) // AES encrypt require 128 or 256 bits for key

	if p.Mode == EncryptAES128 {
		return sum256[:16], nil
	}

	return sum256[:], nil
}

func (p *EncryptionPolicy) readPemFromFile() ([]byte, error) {
	pemData, err := os.ReadFile(*p.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read PEM file: %w", err)
	}

	return pemData, nil
}

// readPemFromEnv reads from env variable encrypted in base64 key body without header and footer,
// decrypt it and adding header and footer.
func (p *EncryptionPolicy) readPemFromEnv() ([]byte, error) {
	key := os.Getenv(*p.KeyEnv)
	if key == "" {
		return nil, fmt.Errorf("environment variable %s not set", *p.KeyEnv)
	}

	// we just add header and footer to make it parsable.
	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}

func (p *EncryptionPolicy) readPemFromSecret(agent *SecretAgentConfig) ([]byte, error) {
	if agent == nil {
		return nil, fmt.Errorf("secret agent not initialized")
	}

	key, err := agent.GetSecret(*p.KeySecret)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret agent key: %w", err)
	}

	pemKey := fmt.Sprintf(pemTemplate, key)

	return []byte(pemKey), nil
}
