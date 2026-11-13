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

package arm

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-logr/logr"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armlocks"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/engine/runner"
	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/prerequisites"
)

// CheckResourcesAbsent accepts only recognized ARM not-found responses. A resource
// whose provisioning state is Deleting still exists and cannot satisfy the prerequisite.
func CheckResourcesAbsent(ctx context.Context, client *armresources.Client, resources []prerequisites.ResourceAbsence) error {
	for _, resource := range resources {
		_, err := client.GetByID(ctx, resource.ID, resource.APIVersion, nil)
		var response *azcore.ResponseError
		if errors.As(err, &response) && response.StatusCode == http.StatusNotFound && (strings.EqualFold(response.ErrorCode, "ResourceNotFound") || strings.EqualFold(response.ErrorCode, "ResourceGroupNotFound") || strings.EqualFold(response.ErrorCode, "ParentResourceNotFound")) {
			continue
		}
		if err != nil {
			return fmt.Errorf("verify absence prerequisite %s: %w", resource.ID, err)
		}
		return fmt.Errorf("absence prerequisite %s still exists; retaining dependent resources", resource.ID)
	}
	return nil
}

// ListByType lists all resources of a specific type in a resource group.
func ListByType(
	ctx context.Context,
	client *armresources.Client,
	resourceGroupName string,
	resourceType string,
) ([]*armresources.GenericResourceExpanded, error) {
	filter := fmt.Sprintf("resourceType eq '%s'", resourceType)
	pager := client.NewListByResourceGroupPager(resourceGroupName, &armresources.ClientListByResourceGroupOptions{
		Filter: &filter,
	})

	var resources []*armresources.GenericResourceExpanded
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list resources of type %s: %w", resourceType, err)
		}
		resources = append(resources, page.Value...)
	}
	return resources, nil
}

// HasLocks reports whether a resource has one or more management locks.
func HasLocks(ctx context.Context, locksClient *armlocks.ManagementLocksClient, resourceID string) (bool, error) {
	pager := locksClient.NewListByScopePager(resourceID, nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to list locks for resource ID %q: %w", resourceID, err)
		}
		if len(page.Value) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// HasResourceGroupLocks reports whether a resource group has management locks.
func HasResourceGroupLocks(ctx context.Context, locksClient *armlocks.ManagementLocksClient, resourceGroupName string) (bool, error) {
	pager := locksClient.NewListAtResourceGroupLevelPager(resourceGroupName, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to list locks for resource group %q: %w", resourceGroupName, err)
		}
		if len(page.Value) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// FilterUnlockedTargets filters out lock-protected targets and logs skips.
func FilterUnlockedTargets(
	ctx context.Context,
	locksClient *armlocks.ManagementLocksClient,
	stepName string,
	targets []runner.Target,
) []runner.Target {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		panic(err)
	}

	filtered := make([]runner.Target, 0, len(targets))
	for _, target := range targets {
		hasLocks, err := HasLocks(ctx, locksClient, target.ID)
		if err != nil {
			logger.Info(
				"Failed to evaluate locks for deletion target; keeping target",
				"step", stepName,
				"resource", target.Name,
				"id", target.ID,
				"error", err,
			)
			filtered = append(filtered, target)
			continue
		}
		if hasLocks {
			logger.Info("Skipping deletion target", "step", stepName, "resource", target.Name, "reason", "locked")
			continue
		}
		filtered = append(filtered, target)
	}
	return filtered
}

// DeleteByID deletes a resource by ARM resource ID and API version.
func DeleteByID(
	ctx context.Context,
	client *armresources.Client,
	resourceID string,
	apiVersion string,
	wait bool,
) error {
	poller, err := client.BeginDeleteByID(ctx, resourceID, apiVersion, nil)
	if err != nil {
		return err
	}
	if wait {
		_, err = poller.PollUntilDone(ctx, nil)
		return err
	}
	return nil
}
