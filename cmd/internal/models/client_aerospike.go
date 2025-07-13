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

package models

import (
	"fmt"

	"github.com/aerospike/tools-common-go/client"
	"github.com/aerospike/tools-common-go/flags"
)

// ClientAerospike wrapper for aerospike client params, to unmarshal YAML.
// Because AerospikeConfig from common library TLS can't be mapped from YAML.
type ClientAerospike struct {
	Seeds []HostTLSPort `yaml:"seeds,omitempty"`

	User     string `yaml:"user,omitempty"`
	Password string `yaml:"password,omitempty"`
	AuthMode string `yaml:"auth,omitempty"`

	Timeout      int64 `yaml:"client-timeout,omitempty"`
	IdleTimeout  int64 `yaml:"client-idle-timeout,omitempty"`
	LoginTimeout int64 `yaml:"client-login-timeout,omitempty"`

	TLS TLSConfig `yaml:"tls,omitempty"`
}

// TLSConfig represents the configuration for enabling TLS with fields for certificates and protocols.
type TLSConfig struct {
	Enable      bool   `yaml:"enable,omitempty"`
	Name        string `yaml:"name,omitempty"`
	Protocols   string `yaml:"protocols,omitempty"`
	RootCAFile  string `yaml:"ca-file,omitempty"`
	RootCAPath  string `yaml:"ca-path,omitempty"`
	CertFile    string `yaml:"cert-file,omitempty"`
	KeyFile     string `yaml:"key-file,omitempty"`
	KeyFilePass string `yaml:"key-file-password,omitempty"`
}

// HostTLSPort defines a structure to represent a host with TLS name and port configuration.
type HostTLSPort struct {
	Host    string `yaml:"host,omitempty"`
	TLSName string `yaml:"tls-name,omitempty"`
	Port    int    `yaml:"port,omitempty"`
}

func (h *HostTLSPort) String() string {
	str := h.Host

	if h.TLSName != "" {
		str = fmt.Sprintf("%s:%s", str, h.TLSName)
	}

	if h.Port != 0 {
		str = fmt.Sprintf("%s:%v", str, h.Port)
	}

	return str
}

// ToConfig maps ClientAerospike to *client.AerospikeConfig so we can use it without any code modifications.
func (c *ClientAerospike) ToConfig() (*client.AerospikeConfig, error) {
	var (
		f         flags.AerospikeFlags
		hostPorts string
	)

	for i := range c.Seeds {
		hostPorts += c.Seeds[i].String()
	}

	if hostPorts != "" {
		var seeds flags.HostTLSPortSliceFlag
		if err := seeds.Set(hostPorts); err != nil {
			return nil, fmt.Errorf("failed to set seeds: %w", err)
		}

		f.Seeds = seeds
	}

	if c.User != "" {
		f.User = c.User
	}

	if c.Password != "" {
		var psw flags.PasswordFlag
		if err := psw.Set(c.Password); err != nil {
			return nil, fmt.Errorf("failed to set password: %w", err)
		}

		f.Password = psw
	}

	if c.AuthMode != "" {
		var authMode flags.AuthModeFlag
		if err := authMode.Set(c.AuthMode); err != nil {
			return nil, fmt.Errorf("failed to set auth mode: %w", err)
		}

		f.AuthMode = authMode
	}

	f.TLSEnable = c.TLS.Enable
	f.TLSName = c.TLS.Name

	if c.TLS.Protocols != "" {
		var tlsProtocols flags.TLSProtocolsFlag
		if err := tlsProtocols.Set(c.TLS.Protocols); err != nil {
			return nil, fmt.Errorf("failed to set tls protocols: %w", err)
		}

		f.TLSProtocols = tlsProtocols
	}

	if c.TLS.RootCAFile != "" {
		var tlsRootCaFile flags.CertFlag

		if err := tlsRootCaFile.Set(c.TLS.RootCAFile); err != nil {
			return nil, fmt.Errorf("failed to set tls root ca file: %w", err)
		}

		f.TLSRootCAFile = tlsRootCaFile
	}

	if c.TLS.RootCAPath != "" {
		var tlsRootCaPath flags.CertPathFlag

		if err := tlsRootCaPath.Set(c.TLS.RootCAPath); err != nil {
			return nil, fmt.Errorf("failed to set tls root ca path: %w", err)
		}

		f.TLSRootCAPath = tlsRootCaPath
	}

	if c.TLS.CertFile != "" {
		var tlsCertFile flags.CertFlag

		if err := tlsCertFile.Set(c.TLS.CertFile); err != nil {
			return nil, fmt.Errorf("failed to set tls cert file: %w", err)
		}

		f.TLSCertFile = tlsCertFile
	}

	if c.TLS.KeyFile != "" {
		var tlsKeyFile flags.CertFlag
		if err := tlsKeyFile.Set(c.TLS.KeyFile); err != nil {
			return nil, fmt.Errorf("failed to set tls key file: %w", err)
		}
	}

	if c.TLS.KeyFilePass != "" {
		var tlsKeyFilePass flags.PasswordFlag
		if err := tlsKeyFilePass.Set(c.TLS.KeyFilePass); err != nil {
			return nil, fmt.Errorf("failed to set tls key file password: %w", err)
		}
	}

	return f.NewAerospikeConfig(), nil
}

// ToClientPolicy return client policy.
func (c *ClientAerospike) ToClientPolicy() *ClientPolicy {
	return &ClientPolicy{
		Timeout:      c.Timeout,
		IdleTimeout:  c.IdleTimeout,
		LoginTimeout: c.LoginTimeout,
	}
}
