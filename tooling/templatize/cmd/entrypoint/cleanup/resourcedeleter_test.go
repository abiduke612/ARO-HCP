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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/prerequisites"
	configtypes "github.com/Azure/ARO-Tools/config/types"
	"github.com/Azure/ARO-Tools/pipelines/graph"
)

func TestBillingCleanupPrerequisites(t *testing.T) {
	const sub = "11111111-1111-1111-1111-111111111111"
	cfg := configtypes.Configuration{
		"billing": map[string]any{"rg": "billing"},
		"svc":     map[string]any{"subscription": map[string]any{"key": "svc-sub"}, "rg": "service", "aks": map[string]any{"name": "service-cluster"}},
	}
	for _, tc := range []struct {
		name, group, subscription string
		want                      []prerequisites.ResourceAbsence
	}{
		{name: "billing producer", group: "billing", subscription: sub, want: []prerequisites.ResourceAbsence{{ID: "/subscriptions/" + sub + "/resourceGroups/service/providers/Microsoft.ContainerService/managedClusters/service-cluster", APIVersion: "2024-10-01", Purpose: prerequisites.Producer}}},
		{name: "unrelated group", group: "global", subscription: sub},
		{name: "same name different subscription", group: "billing", subscription: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := billingCleanupPrerequisites(context.Background(), map[graph.Stamp]configtypes.Configuration{{}: cfg}, tc.group, tc.subscription, func(_ context.Context, key string) (string, error) {
				if key != "svc-sub" {
					t.Fatalf("unexpected subscription key %s", key)
				}
				return sub, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("producer mapping (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBlockedBillingDoesNotCancelSiblingCleanup(t *testing.T) {
	blocked := errors.New("billing producer still exists")
	failed := errors.New("independent service failure")
	for _, tc := range []struct {
		name         string
		serviceError error
	}{
		{name: "service cleanup completes"},
		{name: "both failures are returned", serviceError: failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			billingChecked := make(chan struct{})
			serviceFinished := make(chan struct{})
			operations := []cleanupOperation{
				{name: "billing", logger: logr.Discard(), execute: func(context.Context) error { close(billingChecked); return blocked }},
				{name: "service", logger: logr.Discard(), execute: func(ctx context.Context) error {
					<-billingChecked
					if err := ctx.Err(); err != nil {
						return err
					}
					close(serviceFinished)
					return tc.serviceError
				}},
			}
			err := executeCleanupOperations(ctx, operations)
			if !errors.Is(err, blocked) {
				t.Fatalf("missing billing error: %v", err)
			}
			if tc.serviceError != nil && !errors.Is(err, tc.serviceError) {
				t.Fatalf("missing service error: %v", err)
			}
			select {
			case <-serviceFinished:
			default:
				t.Fatal("billing prerequisite prevented independent service cleanup")
			}
		})
	}
}

func TestRegionalCleanupResourceGroups(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    configtypes.Configuration
		want      []string
		wantError bool
	}{
		{
			name: "regional billing included and shared global excluded",
			config: configtypes.Configuration{
				"regionRG": "region", "svc": map[string]any{"rg": "service"}, "mgmt": map[string]any{"rg": "management"},
				"billing": map[string]any{"rg": "region-billing"}, "global": map[string]any{"billing": map[string]any{"rg": "global-billing"}},
			},
			want: []string{"management", "region", "region-billing", "service"},
		},
		{name: "billing not configured", config: configtypes.Configuration{"regionRG": "region"}, want: []string{"region"}},
		{name: "invalid billing object", config: configtypes.Configuration{"billing": "bad"}, wantError: true},
		{name: "invalid billing name", config: configtypes.Configuration{"billing": map[string]any{"rg": 42}}, wantError: true},
		{name: "empty billing name", config: configtypes.Configuration{"billing": map[string]any{"rg": ""}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := regionalCleanupResourceGroups(map[graph.Stamp]configtypes.Configuration{{}: tc.config})
			if tc.wantError {
				if err == nil {
					t.Fatal("expected invalid configuration to fail")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, sets.List(got)); diff != "" {
				t.Fatalf("resource groups (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourceDeletionOrder(t *testing.T) {
	// Document the deletion order - this serves as living documentation
	// The order matters due to Azure resource dependencies
	steps := []struct {
		step        string
		description string
		critical    bool
	}{
		{
			step:        "Step 1",
			description: "Network Security Perimeters (NSP) with force deletion",
			critical:    true,
		},
		{
			step:        "Step 2a",
			description: "Private DNS Zone Groups",
			critical:    true,
		},
		{
			step:        "Step 2b",
			description: "Private Endpoint Connections",
			critical:    true,
		},
		{
			step:        "Step 2c",
			description: "Private Endpoints",
			critical:    true,
		},
		{
			step:        "Step 2d",
			description: "Private DNS Zone Virtual Network Links",
			critical:    true,
		},
		{
			step:        "Step 2e",
			description: "Private Link Services",
			critical:    true,
		},
		{
			step:        "Step 2f",
			description: "Private DNS Zones",
			critical:    true,
		},
		{
			step:        "Step 3",
			description: "Public DNS Zones",
			critical:    false,
		},
		{
			step:        "Step 4",
			description: "Application Resources (bulk deletion - AKS, Cosmos, etc.)",
			critical:    false,
		},
		{
			step:        "Step 4b",
			description: "Public IP Addresses (with 3 retries after AKS deletion)",
			critical:    true,
		},
		{
			step:        "Step 5",
			description: "Monitoring Resources (Data Collection Rules & Endpoints)",
			critical:    false,
		},
		{
			step:        "Step 6",
			description: "Core Networking (Virtual Networks, NSGs)",
			critical:    true,
		},
		{
			step:        "Step 7",
			description: "Key Vault Purge (soft-deleted vaults)",
			critical:    false,
		},
		{
			step:        "Step 8",
			description: "Resource Group Deletion (with 5 retries)",
			critical:    true,
		},
	}

	assert.Equal(t, 14, len(steps), "Deletion process should have 14 steps")

	t.Log("Documented deletion order:")
	for _, step := range steps {
		criticalMarker := ""
		if step.critical {
			criticalMarker = " [CRITICAL ORDER]"
		}
		t.Logf("  %s: %s%s", step.step, step.description, criticalMarker)
	}
}
