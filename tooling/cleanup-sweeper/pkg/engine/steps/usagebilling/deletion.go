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

package usagebilling

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armlocks"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/engine/runner"
	armsteps "github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/engine/steps/arm"
	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/prerequisites"
)

const (
	accountType = "Microsoft.UsageBilling/accounts"
	apiVersion  = "2025-01-01-preview"
)

type deletionStep struct {
	resourceGroup string
	resources     *armresources.Client
	locks         *armlocks.ManagementLocksClient
	arm           *azcorearm.Client
	requireAbsent []prerequisites.ResourceAbsence
}

var _ runner.Step = (*deletionStep)(nil)

// NewDeletionStep handles AUB's child dependencies before generic resource-group deletion.
// Resource inventory cannot enumerate all AUB children; the provider's collection APIs are required.
func NewDeletionStep(resourceGroup string, resources *armresources.Client, locks *armlocks.ManagementLocksClient, credential azcore.TokenCredential, options *azcorearm.ClientOptions, requireAbsent []prerequisites.ResourceAbsence) (runner.Step, error) {
	if len(strings.TrimSpace(resourceGroup)) == 0 || resources == nil || locks == nil || credential == nil {
		return nil, errors.New("resource group, resources client, locks client and credential are required")
	}
	client, err := azcorearm.NewClient("cleanup-sweeper-usagebilling", "v1.0.0", credential, options)
	if err != nil {
		return nil, fmt.Errorf("create AUB collection client: %w", err)
	}
	for _, prerequisite := range requireAbsent {
		if err := prerequisite.Validate(); err != nil {
			return nil, err
		}
	}
	return &deletionStep{resourceGroup: resourceGroup, resources: resources, locks: locks, arm: client, requireAbsent: slices.Clone(requireAbsent)}, nil
}

func (s *deletionStep) Name() string    { return "Delete Azure Usage Billing accounts" }
func (s *deletionStep) RetryLimit() int { return 1 }

// AUB failure must block subsequent generic deletion, even in a best-effort workflow.
func (s *deletionStep) ContinueOnError() bool { return false }

func (s *deletionStep) Discover(ctx context.Context) ([]runner.Target, error) {
	resources, err := armsteps.ListByType(ctx, s.resources, s.resourceGroup, accountType)
	if err != nil {
		return nil, err
	}
	var targets []runner.Target
	for _, resource := range resources {
		if resource == nil || resource.ID == nil || resource.Name == nil || resource.Type == nil {
			return nil, errors.New("incomplete AUB account inventory")
		}
		id, err := azcorearm.ParseResourceID(*resource.ID)
		if err != nil || !strings.EqualFold(id.ResourceType.String(), accountType) || !strings.EqualFold(id.ResourceGroupName, s.resourceGroup) {
			return nil, fmt.Errorf("unexpected account resource ID %q", *resource.ID)
		}
		targets = append(targets, runner.Target{ID: *resource.ID, Name: *resource.Name, Type: accountType})
	}
	if len(targets) != 0 {
		if err := s.checkProducerPrerequisites(ctx); err != nil {
			return nil, err
		}
	}
	return targets, nil
}

// A generic dependency is insufficient: HCP billing requires an explicitly identified
// producer AKS cluster. Configuration owns the association; names cannot prove ownership.
func (s *deletionStep) checkProducerPrerequisites(ctx context.Context) error {
	producers := 0
	for _, prerequisite := range s.requireAbsent {
		if prerequisite.Purpose != prerequisites.Producer {
			continue
		}
		id, err := azcorearm.ParseResourceID(prerequisite.ID)
		if err != nil || !strings.EqualFold(id.ResourceType.String(), "Microsoft.ContainerService/managedClusters") {
			return fmt.Errorf("AUB producer prerequisite must identify an AKS cluster: %s", prerequisite.ID)
		}
		producers++
	}
	if producers == 0 {
		return errors.New("AUB cleanup requires producer absence prerequisites")
	}
	return armsteps.CheckResourcesAbsent(ctx, s.resources, s.requireAbsent)
}

func (s *deletionStep) Verify(ctx context.Context) error {
	remaining, err := s.Discover(ctx)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return fmt.Errorf("%d AUB accounts remain; refusing to continue resource-group deletion", len(remaining))
	}
	return nil
}

type child struct {
	id       string
	selector bool
}

// Delete waits for AUB operations even when general cleanup is asynchronous. AUB rejects
// account deletion while pipelines/inputs/PAv2Output remain and PAv2Output deletion while
// selectors remain. The caller must stop producers and resolve pending usage before teardown.
// https://eng.ms/docs/cloud-ai-platform/azure-data/azure-data-azure-usage-billing/azure-usage-billing/azure-usage-billing/azure-usage-billing-customer-docs/customer/howto/commerce-integration/setup-pav2-integration
// https://msdata.visualstudio.com/UsageBilling/_git/usage-billing?path=/src/Microsoft.Azure.UsageBilling.UsagePipeline.Operation.Logical/UsageAccount/LogicalModelOperationService.cs&version=GCb3c4e1344b8c7bd9ab673633067c2b47ac8b665b&line=99&lineEnd=121
func (s *deletionStep) Delete(ctx context.Context, target runner.Target, _ bool) error {
	id, err := azcorearm.ParseResourceID(target.ID)
	if err != nil || !strings.EqualFold(id.ResourceType.String(), accountType) || !strings.EqualFold(id.ResourceGroupName, s.resourceGroup) {
		return fmt.Errorf("invalid AUB deletion target %q", target.ID)
	}
	account, err := s.get(ctx, target.ID)
	if err != nil {
		return err
	}
	if account == nil {
		return runner.ErrTargetAbsent
	}
	if err := s.checkProducerPrerequisites(ctx); err != nil {
		return err
	}
	if err := s.checkLocks(ctx, target.ID); err != nil {
		return err
	}
	pipelines, err := s.list(ctx, target.ID, "pipelines")
	if err != nil {
		return err
	}
	var selectors []child
	for _, pipeline := range pipelines {
		items, err := s.list(ctx, pipeline.id, "outputSelectors")
		if err != nil {
			return err
		}
		selectors = append(selectors, items...)
	}
	outputs, err := s.list(ctx, target.ID, "pav2outputs")
	if err != nil {
		return err
	}
	// Build the complete ordered inventory before performing any mutation.
	ordered := append(append(selectors, outputs...), pipelines...)
	for _, collection := range []string{"inputs", "dataExports", "metricExports"} {
		items, err := s.list(ctx, target.ID, collection)
		if err != nil {
			return err
		}
		ordered = append(ordered, items...)
	}
	for _, item := range ordered {
		if err := s.checkLocks(ctx, item.id); err != nil {
			return err
		}
		if item.selector {
			resource, err := s.get(ctx, item.id)
			if err != nil {
				return err
			}
			if resource != nil {
				if _, err := closedSelector(resource.Properties, time.Now().UTC()); err != nil {
					return fmt.Errorf("validate selector %s: %w", item.id, err)
				}
			}
		}
	}
	for _, item := range ordered {
		if err := s.deleteResource(ctx, item); err != nil {
			return err
		}
	}
	return s.deleteResource(ctx, child{id: target.ID})
}

func (s *deletionStep) checkLocks(ctx context.Context, id string) error {
	groupLocked, err := armsteps.HasResourceGroupLocks(ctx, s.locks, s.resourceGroup)
	if err != nil {
		return err
	}
	if groupLocked {
		return fmt.Errorf("resource group %s has management locks; retaining AUB resources", s.resourceGroup)
	}
	locked, err := armsteps.HasLocks(ctx, s.locks, id)
	if err != nil {
		return err
	}
	if locked {
		return fmt.Errorf("resource %s has management locks; retaining AUB resources", id)
	}
	return nil
}

func (s *deletionStep) get(ctx context.Context, id string) (*armresources.GenericResource, error) {
	response, err := s.resources.GetByID(ctx, id, apiVersion, nil)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", id, err)
	}
	if response.ID == nil || !strings.EqualFold(*response.ID, id) {
		return nil, fmt.Errorf("response ID does not match %s", id)
	}
	properties, ok := response.Properties.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("resource %s has unexpected properties type %T", id, response.Properties)
	}
	state, _ := properties["provisioningState"].(string)
	if strings.EqualFold(state, "Deleting") || strings.EqualFold(state, "Updating") || strings.EqualFold(state, "Creating") {
		return nil, fmt.Errorf("resource %s is %s; wait for its current operation before retrying", id, state)
	}
	return &response.GenericResource, nil
}

type resourcePage struct {
	Value    []*armresources.GenericResource `json:"value"`
	NextLink string                          `json:"nextLink"`
}

func (s *deletionStep) list(ctx context.Context, parent, collection string) ([]child, error) {
	collectionURL := runtime.JoinPaths(s.arm.Endpoint(), parent, collection)
	expected, err := url.Parse(collectionURL)
	if err != nil {
		return nil, err
	}
	pager := runtime.NewPager(runtime.PagingHandler[resourcePage]{
		More: func(page resourcePage) bool { return len(page.NextLink) != 0 },
		Fetcher: func(ctx context.Context, page *resourcePage) (resourcePage, error) {
			endpoint := collectionURL + "?api-version=" + apiVersion
			if page != nil {
				endpoint = page.NextLink
			}
			next, err := url.Parse(endpoint)
			if err != nil || next.Scheme != expected.Scheme || next.Host != expected.Host || !strings.EqualFold(next.Path, expected.Path) || next.User != nil {
				return resourcePage{}, fmt.Errorf("unexpected AUB pagination URL for %s/%s", parent, collection)
			}
			request, err := runtime.NewRequest(ctx, http.MethodGet, endpoint)
			if err != nil {
				return resourcePage{}, err
			}
			response, err := s.arm.Pipeline().Do(request)
			if err != nil {
				return resourcePage{}, err
			}
			if !runtime.HasStatusCode(response, http.StatusOK) {
				return resourcePage{}, runtime.NewResponseError(response)
			}
			var result resourcePage
			if err := runtime.UnmarshalAsJSON(response, &result); err != nil {
				return resourcePage{}, err
			}
			if result.Value == nil {
				return resourcePage{}, errors.New("AUB collection response is missing its value array")
			}
			return result, nil
		},
	})
	var result []child
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list %s/%s: %w", parent, collection, err)
		}
		for _, resource := range page.Value {
			if resource == nil || resource.ID == nil {
				return nil, fmt.Errorf("incomplete resource in %s/%s", parent, collection)
			}
			prefix := parent + "/" + collection + "/"
			if !strings.HasPrefix(strings.ToLower(*resource.ID), strings.ToLower(prefix)) {
				return nil, fmt.Errorf("unexpected resource %s in %s/%s", *resource.ID, parent, collection)
			}
			name := (*resource.ID)[len(prefix):]
			if len(name) == 0 || name == "." || name == ".." || strings.ContainsAny(name, "/\\%?#") {
				return nil, fmt.Errorf("invalid child resource ID %q", *resource.ID)
			}
			result = append(result, child{id: *resource.ID, selector: collection == "outputSelectors"})
		}
	}
	return result, nil
}

// closedSelector produces the PUT representation without modifying the GET response.
// Live selectors must be closed before DELETE; GET's output type is not a valid PUT path.
// https://eng.ms/docs/cloud-ai-platform/azure-data/azure-data-azure-usage-billing/azure-usage-billing/azure-usage-billing/azure-usage-billing-customer-docs/customer/howto/commerce-integration/setup-commerce-output
// https://msdata.visualstudio.com/UsageBilling/_git/usage-billing?path=/src/Microsoft.Azure.UsageBilling.UsagePipeline.ResourceProvider/Api/OutputSelectors/20231201/OutputSelectorConverters20231201.cs&version=GCb3c4e1344b8c7bd9ab673633067c2b47ac8b665b&line=50&lineEnd=55
func closedSelector(raw any, now time.Time) (map[string]any, error) {
	properties, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected selector properties type %T", raw)
	}
	if properties["categorySettings"] != nil {
		return nil, errors.New("category-based selectors require explicit category retirement")
	}
	start, ok := properties["startDate"].(string)
	if !ok {
		return nil, errors.New("missing selector startDate")
	}
	startTime, err := time.Parse(time.RFC3339Nano, start)
	if err != nil || !startTime.Before(now) {
		return nil, errors.New("selector startDate must be a valid past timestamp")
	}
	output, _ := properties["outputId"].(string)
	if !strings.EqualFold(output, "pav2outputs") && !strings.HasSuffix(strings.ToLower(output), "/pav2outputs/default") {
		return nil, fmt.Errorf("unsupported selector output %q", output)
	}
	if end := properties["endDate"]; end != nil {
		value, ok := end.(string)
		if !ok {
			return nil, errors.New("invalid selector endDate type")
		}
		endTime, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return nil, fmt.Errorf("invalid selector endDate: %w", err)
		}
		if !endTime.After(now) {
			return nil, nil
		}
	}
	return map[string]any{"startDate": start, "endDate": now.Format(time.RFC3339Nano), "outputId": "/pav2outputs/default"}, nil
}

func (s *deletionStep) deleteResource(ctx context.Context, item child) error {
	logger := logr.FromContextOrDiscard(ctx).WithValues("resourceId", item.id)
	resource, err := s.get(ctx, item.id)
	if err != nil || resource == nil {
		return err
	}
	if err := s.checkLocks(ctx, item.id); err != nil {
		return err
	}
	if err := s.checkProducerPrerequisites(ctx); err != nil {
		return err
	}
	if item.selector {
		properties, err := closedSelector(resource.Properties, time.Now().UTC())
		if err != nil {
			return fmt.Errorf("close selector %s: %w", item.id, err)
		}
		if properties != nil {
			logger.Info("Closing AUB output selector")
			poller, err := s.resources.BeginCreateOrUpdateByID(ctx, item.id, apiVersion, armresources.GenericResource{Properties: properties}, nil)
			if err != nil {
				return fmt.Errorf("close selector %s: %w", item.id, err)
			}
			if _, err := poller.PollUntilDone(ctx, nil); err != nil {
				return fmt.Errorf("wait for selector close %s: %w", item.id, err)
			}
			closed, err := s.get(ctx, item.id)
			if err != nil || closed == nil {
				return err
			}
			pending, err := closedSelector(closed.Properties, time.Now().UTC())
			if err != nil {
				return err
			}
			if pending != nil {
				return fmt.Errorf("selector %s remains open after update", item.id)
			}
		}
	}
	logger.Info("Deleting AUB resource")
	if err := s.checkProducerPrerequisites(ctx); err != nil {
		return err
	}
	poller, err := s.resources.BeginDeleteByID(ctx, item.id, apiVersion, nil)
	if isNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete %s: %w", item.id, err)
	}
	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		return fmt.Errorf("wait for deletion %s: %w", item.id, err)
	}
	remaining, err := s.get(ctx, item.id)
	if err != nil {
		return err
	}
	if remaining != nil {
		return fmt.Errorf("resource %s remains after deletion (possibly soft-deleted); refusing to continue", item.id)
	}
	return nil
}

func isNotFound(err error) bool {
	var response *azcore.ResponseError
	return errors.As(err, &response) && response.StatusCode == http.StatusNotFound
}
