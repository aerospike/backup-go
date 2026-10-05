// Copyright 2024-2026 Aerospike, Inc.
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

package asinfo

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	cmdNameSep  = ':'
	cmdParamSep = ';'
	cmdValueSep = '='
	cmdPathSep  = '/'

	// cmdUnsafeChars must not appear in a value: ";" would split it into extra
	// parameters, "\n" into extra commands of the info request.
	cmdUnsafeChars = ";\n"
)

// infoCmd builds an info command in the form "name:key1=value1;key2=value2".
// String parameters with empty values are omitted, so the server receives only
// the fields that were actually set.
type infoCmd struct {
	b         strings.Builder
	name      string
	unsafe    string
	missing   []string
	invalid   []string
	hasParams bool
}

// newInfoCmd starts the command name. No value of the command may contain
// cmdUnsafeChars, nor any of the extra forbidden characters.
func newInfoCmd(name string, forbidden ...string) *infoCmd {
	c := &infoCmd{name: name, unsafe: cmdUnsafeChars + strings.Join(forbidden, "")}
	c.b.WriteString(name)
	c.b.WriteByte(cmdNameSep)

	return c
}

// str adds the parameter only if value is not empty.
func (c *infoCmd) str(key, value string) *infoCmd {
	if value == "" {
		return c
	}

	c.add(key, value)

	return c
}

// required adds the parameter like str, and records it as missing if value is
// empty. Missing parameters are reported by build.
func (c *infoCmd) required(key, value string) *infoCmd {
	if value == "" {
		c.missing = append(c.missing, key)

		return c
	}

	c.add(key, value)

	return c
}

// flag adds "key=true" or "key=false". It is sent unconditionally, so the
// server never falls back to its own default.
func (c *infoCmd) flag(key string, value bool) *infoCmd {
	c.add(key, strconv.FormatBool(value))

	return c
}

// optFlag adds "key=true" or "key=false" only if value is set, so the server
// applies its own default otherwise.
func (c *infoCmd) optFlag(key string, value *bool) *infoCmd {
	if value == nil {
		return c
	}

	return c.flag(key, *value)
}

// num adds the integer parameter unconditionally.
func (c *infoCmd) num(key string, value int) *infoCmd {
	c.add(key, strconv.Itoa(value))

	return c
}

// optNum adds the integer parameter only if value is not zero, so the server
// applies its own default otherwise.
func (c *infoCmd) optNum(key string, value int) *infoCmd {
	if value == 0 {
		return c
	}

	return c.num(key, value)
}

// optFloat adds the float parameter in its shortest exact form, only if value
// is not zero, so the server applies its own default otherwise.
func (c *infoCmd) optFloat(key string, value float64) *infoCmd {
	if value == 0 {
		return c
	}

	c.add(key, strconv.FormatFloat(value, 'f', -1, 64))

	return c
}

// build returns the command, or an error if any parameter added with required
// was empty (errMissingCmdParam) or any value contains a forbidden character
// (errInvalidCmdParam), see newInfoCmd. Only the keys are reported, values may
// hold secrets.
func (c *infoCmd) build() (string, error) {
	switch {
	case len(c.missing) > 0 && len(c.invalid) > 0:
		return "", fmt.Errorf("%w; %w", paramsErr(errMissingCmdParam, c.name, c.missing),
			paramsErr(errInvalidCmdParam, c.name, c.invalid))
	case len(c.missing) > 0:
		return "", paramsErr(errMissingCmdParam, c.name, c.missing)
	case len(c.invalid) > 0:
		return "", paramsErr(errInvalidCmdParam, c.name, c.invalid)
	}

	return c.b.String(), nil
}

func (c *infoCmd) add(key, value string) {
	if strings.ContainsAny(value, c.unsafe) {
		c.invalid = append(c.invalid, key)
	}

	if c.hasParams {
		c.b.WriteByte(cmdParamSep)
	}

	c.b.WriteString(key)
	c.b.WriteByte(cmdValueSep)
	c.b.WriteString(value)
	c.hasParams = true
}

// buildPathCmd returns a path-style command "name/value", such as "sets/<ns>".
// The value is validated like a parameter added with required under key.
func buildPathCmd(name, key, value string) (string, error) {
	switch {
	case value == "":
		return "", paramsErr(errMissingCmdParam, name, []string{key})
	case strings.ContainsAny(value, cmdUnsafeChars):
		return "", paramsErr(errInvalidCmdParam, name, []string{key})
	}

	return name + string(cmdPathSep) + value, nil
}

// paramsErr wraps sentinel with the command name and the offending keys.
func paramsErr(sentinel error, name string, keys []string) error {
	return fmt.Errorf("%w: %s: %s", sentinel, name, strings.Join(keys, ", "))
}
