// Copyright 2026 Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prerequisites

import (
	"fmt"
	"strings"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/google/uuid"
)

// ResourceAbsence is configuration, independent of the engine that evaluates it.
// Purpose identifies the resource's relationship to the retiring resources.
type ResourceAbsence struct {
	ID         string `json:"id" yaml:"id"`
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Purpose    string `json:"purpose,omitempty" yaml:"purpose,omitempty"`
}

const Producer = "producer"

func (r ResourceAbsence) Validate() error {
	id, err := azcorearm.ParseResourceID(r.ID)
	if err != nil || len(id.ResourceGroupName) == 0 || strings.TrimSpace(r.ID) != r.ID || strings.ContainsAny(r.ID, "?#%\\") || strings.HasSuffix(r.ID, "/") || strings.Contains(r.ID, "//") {
		return fmt.Errorf("invalid absence prerequisite resource ID %q", r.ID)
	}
	if sub, err := uuid.Parse(id.SubscriptionID); err != nil || sub == uuid.Nil {
		return fmt.Errorf("absence prerequisite requires a subscription UUID: %q", r.ID)
	}
	for _, part := range strings.Split(r.ID, "/") {
		if part == "." || part == ".." {
			return fmt.Errorf("invalid absence prerequisite resource ID %q", r.ID)
		}
	}
	if len(strings.TrimSpace(r.APIVersion)) == 0 || strings.TrimSpace(r.APIVersion) != r.APIVersion {
		return fmt.Errorf("absence prerequisite %s requires an API version", r.ID)
	}
	if r.Purpose != "" && r.Purpose != Producer {
		return fmt.Errorf("unsupported absence prerequisite purpose %q", r.Purpose)
	}
	return nil
}
