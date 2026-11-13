// Copyright 2025 Microsoft Corporation
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

package cleanup

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/spf13/cobra"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	configtypes "github.com/Azure/ARO-Tools/config/types"
	"github.com/Azure/ARO-Tools/pipelines/graph"
	"github.com/Azure/ARO-Tools/pipelines/topology"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/prerequisites"
	"github.com/Azure/ARO-HCP/tooling/templatize/cmd/entrypoint/entrypointutils"
	"github.com/Azure/ARO-HCP/tooling/templatize/pkg/pipeline"
)

const defaultCleanupParallelism = 8

func DefaultOptions() *RawOptions {
	return &RawOptions{
		RawOptions:   entrypointutils.DefaultOptions(),
		OnlyRegional: true,
		Parallelism:  defaultCleanupParallelism,
	}
}

func BindOptions(opts *RawOptions, cmd *cobra.Command) error {
	if err := entrypointutils.BindOptions(opts.RawOptions, cmd); err != nil {
		return err
	}

	cmd.Flags().BoolVar(&opts.OnlyRegional, "only-regional", opts.OnlyRegional, "Only cleanup resources from regional resource groups.")

	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", opts.DryRun, "Print the resource groups that would be cleaned up without deleting them.")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the resource groups to be fully cleaned up.")
	cmd.Flags().IntVar(&opts.Parallelism, "parallelism", opts.Parallelism, "Maximum number of deletions to run in parallel per cleanup step.")

	return nil
}

type RawOptions struct {
	*entrypointutils.RawOptions

	OnlyRegional bool

	DryRun bool
	Wait   bool

	Parallelism int
}

// validatedOptions is a private wrapper that enforces a call of Validate() before Complete() can be invoked.
type validatedOptions struct {
	*RawOptions
	*entrypointutils.ValidatedOptions
}

type ValidatedOptions struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*validatedOptions
}

// completedOptions is a private wrapper that enforces a call of Complete() before config generation can be invoked.
type completedOptions struct {
	*entrypointutils.Options

	AzureCredential    azcore.TokenCredential
	SubscriptionLookup pipeline.SubscriptionLookup

	OnlyRegional bool

	DryRun bool
	Wait   bool

	Parallelism int
}

type Options struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedOptions
}

func (o *RawOptions) Validate(ctx context.Context) (*ValidatedOptions, error) {
	validated, err := o.RawOptions.Validate(ctx)
	if err != nil {
		return nil, err
	}
	if o.Parallelism < 1 {
		return nil, fmt.Errorf("--parallelism must be >= 1, got %d", o.Parallelism)
	}

	return &ValidatedOptions{
		validatedOptions: &validatedOptions{
			RawOptions:       o,
			ValidatedOptions: validated,
		},
	}, nil
}

func (o *ValidatedOptions) Complete(ctx context.Context) (*Options, error) {
	completed, err := o.ValidatedOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	azCredential, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{RequireAzureTokenCredentials: true})
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	return &Options{
		completedOptions: &completedOptions{
			Options: completed,

			AzureCredential:    azCredential,
			SubscriptionLookup: pipeline.LookupSubscriptionID(o.Subscriptions),

			OnlyRegional: o.OnlyRegional,

			DryRun: o.DryRun,
			Wait:   o.Wait,

			Parallelism: o.Parallelism,
		},
	}, nil
}

func (o *Options) CleanUpResources(ctx context.Context) error {
	logger, err := logr.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to get logger from context: %w", err)
	}

	var executionGraph *graph.Graph
	if o.Entrypoint != nil {
		executionGraph, err = graph.ForStampedEntrypoints(&o.Topo.Topology, []*topology.Entrypoint{o.Entrypoint}, o.StampPipelines)
	} else {
		executionGraph, err = graph.ForStampedPipeline(o.Service, o.StampPipelines)
	}
	if err != nil {
		return fmt.Errorf("failed to generate execution graph: %w", err)
	}

	var regionalRGs sets.Set[string]
	if o.OnlyRegional {
		regionalRGs, err = regionalCleanupResourceGroups(o.StampConfigs)
		if err != nil {
			return err
		}
	}

	var planned []cleanupOperation

	sortedKeys := slices.SortedFunc(maps.Keys(executionGraph.ResourceGroups), func(a, b graph.ResourceGroupKey) int {
		if c := cmp.Compare(a.Stamp.String(), b.Stamp.String()); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})

	for _, key := range sortedKeys {
		resourceGroup := executionGraph.ResourceGroups[key]
		rgLogger := logger.WithValues("resourceGroup", resourceGroup.ResourceGroup)

		if o.OnlyRegional && !regionalRGs.Has(resourceGroup.ResourceGroup) {
			rgLogger.Info("Skipping non-regional resource group")
			continue
		}

		// In dry-run mode without wait, just log what would be deleted and continue
		if o.DryRun && !o.Wait {
			rgLogger.Info("Would delete resource group.", "resourceGroup", resourceGroup.ResourceGroup)
			continue
		}

		subscriptionID, err := o.SubscriptionLookup(ctx, resourceGroup.Subscription)
		if err != nil {
			return fmt.Errorf("failed to lookup subscription ID for %q: %w", resourceGroup.Subscription, err)
		}
		requireAbsent, err := billingCleanupPrerequisites(ctx, o.StampConfigs, resourceGroup.ResourceGroup, subscriptionID, o.SubscriptionLookup)
		if err != nil {
			return err
		}

		// Create deleter for this resource group
		deleter := &resourceGroupDeleter{
			requireAbsent:     requireAbsent,
			resourceGroupName: resourceGroup.ResourceGroup,
			subscriptionID:    subscriptionID,
			credential:        o.AzureCredential,
			wait:              o.Wait,
			dryRun:            o.DryRun,
			parallelism:       o.Parallelism,
		}

		planned = append(planned, cleanupOperation{name: subscriptionID + "/" + resourceGroup.ResourceGroup, logger: rgLogger.WithValues("subscriptionID", subscriptionID), execute: deleter.execute})
	}

	if err := executeCleanupOperations(ctx, planned); err != nil {
		return err
	}

	if !o.DryRun {
		if err := os.RemoveAll(o.StepCacheDir); err != nil {
			return fmt.Errorf("failed to remove cache dir %s: %w", o.StepCacheDir, err)
		}
		logger.Info("Cleaned up step cache dir.", "dir", o.StepCacheDir)
	}
	return nil
}

func billingCleanupPrerequisites(ctx context.Context, configs map[graph.Stamp]configtypes.Configuration, resourceGroup, subscriptionID string, lookup pipeline.SubscriptionLookup) ([]prerequisites.ResourceAbsence, error) {
	resources := map[string]prerequisites.ResourceAbsence{}
	for _, cfg := range configs {
		billingRG, err := cfg.GetByPath("billing.rg")
		var missing *configtypes.MissingKeyError
		if errors.As(err, &missing) {
			continue
		}
		if err != nil {
			return nil, err
		}
		name, ok := billingRG.(string)
		if !ok {
			return nil, fmt.Errorf("billing.rg must be a string, got %T", billingRG)
		}
		if !strings.EqualFold(name, resourceGroup) {
			continue
		}
		values := make(map[string]string)
		for _, path := range []string{"svc.subscription.key", "svc.rg", "svc.aks.name"} {
			value, err := cfg.GetByPath(path)
			if err != nil {
				return nil, fmt.Errorf("derive billing producer: %w", err)
			}
			text, ok := value.(string)
			if !ok || len(strings.TrimSpace(text)) == 0 || strings.ContainsAny(text, "/\\?#%") {
				return nil, fmt.Errorf("invalid %s for billing producer", path)
			}
			values[path] = text
		}
		svcSub, err := lookup(ctx, values["svc.subscription.key"])
		if err != nil {
			return nil, fmt.Errorf("resolve billing producer subscription: %w", err)
		}
		// Billing.Regional deploys billing.rg into the service subscription.
		if !strings.EqualFold(svcSub, subscriptionID) {
			continue
		}
		id := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.ContainerService/managedClusters/%s", svcSub, values["svc.rg"], values["svc.aks.name"])
		prerequisite := prerequisites.ResourceAbsence{ID: id, APIVersion: "2024-10-01", Purpose: prerequisites.Producer}
		if err := prerequisite.Validate(); err != nil {
			return nil, err
		}
		resources[strings.ToLower(id)] = prerequisite
	}
	return slices.SortedFunc(maps.Values(resources), func(a, b prerequisites.ResourceAbsence) int { return strings.Compare(a.ID, b.ID) }), nil
}

type cleanupOperation struct {
	name    string
	logger  logr.Logger
	execute func(context.Context) error
}

// A blocked dependency must not cancel the sibling cleanup that can satisfy it.
// Resolve and validate the entire plan before starting independent RG operations.
func executeCleanupOperations(ctx context.Context, operations []cleanupOperation) error {
	var workers sync.WaitGroup
	results := make(chan error, len(operations))
	for _, operation := range operations {
		workers.Add(1)
		go func() {
			defer utilruntime.HandleCrash()
			defer workers.Done()
			if err := operation.execute(logr.NewContext(ctx, operation.logger)); err != nil {
				results <- fmt.Errorf("cleanup %s: %w", operation.name, err)
			}
		}()
	}
	workers.Wait()
	close(results)
	var failures []error
	for err := range results {
		failures = append(failures, err)
	}
	return errors.Join(append(failures, ctx.Err())...)
}

func regionalCleanupResourceGroups(configs map[graph.Stamp]configtypes.Configuration) (sets.Set[string], error) {
	names := sets.New(entrypointutils.RegionalResourceGroupNames(configs)...)
	for _, cfg := range configs {
		billingRG, err := cfg.GetByPath("billing.rg")
		var missing *configtypes.MissingKeyError
		if errors.As(err, &missing) {
			continue // Billing is optional in topologies that do not deploy it.
		}
		if err != nil {
			return nil, fmt.Errorf("read billing.rg: %w", err)
		}
		name, ok := billingRG.(string)
		if !ok || len(name) == 0 {
			return nil, fmt.Errorf("billing.rg must be a nonempty string, got %T", billingRG)
		}
		names.Insert(name)
	}
	return names, nil
}
