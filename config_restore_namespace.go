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

package backup

import "fmt"

// RestoreNamespaceConfig specifies an alternative namespace name for the restore
// operation, where Source is the original namespace name and Destination is
// the namespace name to which the backup data is to be restored.
//
// @Description RestoreNamespaceConfig specifies an alternative namespace name for the restore
// @Description operation.
type RestoreNamespaceConfig struct {
	// Original namespace name.
	Source *string `json:"source,omitempty" example:"source-ns" validate:"required"`
	// Destination namespace name.
	Destination *string `json:"destination,omitempty" example:"destination-ns" validate:"required"`
}

// Validate validates the restore namespace.
func (n *RestoreNamespaceConfig) Validate() error {
	if n.Source == nil {
		return fmt.Errorf("source namespace is not specified")
	}

	if n.Destination == nil {
		return fmt.Errorf("destination namespace is not specified")
	}

	return nil
}
