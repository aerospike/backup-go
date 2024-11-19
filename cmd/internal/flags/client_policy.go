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

package flags

import (
	"github.com/aerospike/backup-go/cmd/internal/models"
	"github.com/spf13/pflag"
)

type ClientPolicy struct {
	models.ClientPolicy
}

func NewClientPolicy() *ClientPolicy {
	return &ClientPolicy{}
}

func (f *ClientPolicy) NewFlagSet() *pflag.FlagSet {
	flagSet := &pflag.FlagSet{}

	flagSet.Int64Var(&f.Timeout, "client-timeout",
		30000,
		"Initial host connection timeout duration. The timeout when opening a connection\n"+
			"to the server host for the first time.")
	flagSet.Int64Var(&f.IdleTimeout, "client-idle-timeout",
		0,
		"Idle timeout. Every time a connection is used, its idle\n"+
			"deadline will be extended by this duration. When this deadline is reached,\n"+
			"the connection will be closed and discarded from the connection pool.\n"+
			"The value is limited to 24 hours (86400s).\n"+
			"It's important to set this value to a few seconds less than the server's proto-fd-idle-ms\n"+
			"(default 60000 milliseconds or 1 minute), so the client does not attempt to use a socket\n"+
			"that has already been reaped by the server.\n"+
			"Connection pools are now implemented by a LIFO stack. Connections at the tail of the\n"+
			"stack will always be the least used. These connections are checked for IdleTimeout\n"+
			"on every tend (usually 1 second).\n")
	flagSet.Int64Var(&f.LoginTimeout, "client-login-timeout",
		10000,
		"specifies the timeout for login operation for external authentication such as LDAP.")

	return flagSet
}

func (f *ClientPolicy) GetClientPolicy() *models.ClientPolicy {
	return &f.ClientPolicy
}
