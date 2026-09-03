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

package asinfo

import (
	"regexp"
	"strings"
)

// redactedValue is the placeholder that replaces sensitive parameter values.
const redactedValue = "[REDACTED]"

// sensitiveParams lists the info command parameters whose values must never
// appear in error messages.
//
// To cover a new parameter, add its name to this list. Nothing else has to
// change: the matcher is built from it and every redaction site uses redactCmd.
var sensitiveParams = []string{
	"access-key",
	"secret-key",
}

// sensitiveParamsRegex matches the values of sensitiveParams.
var sensitiveParamsRegex = compileSensitiveParams(sensitiveParams)

// compileSensitiveParams builds the matcher for the values of the given
// parameters. A value runs until ";", the info protocol parameter separator,
// which is why a value can never legally contain it.
func compileSensitiveParams(params []string) *regexp.Regexp {
	names := make([]string, 0, len(params))
	for _, p := range params {
		names = append(names, regexp.QuoteMeta(p))
	}

	return regexp.MustCompile("(" + strings.Join(names, "|") + ")=[^;]*")
}

// redactCmd masks the values of sensitiveParams in an info command, or in a
// server response that echoes the command back. The result is safe to embed
// into errors that propagate to the callers of this package.
func redactCmd(cmd string) string {
	return sensitiveParamsRegex.ReplaceAllString(cmd, "${1}="+redactedValue)
}
