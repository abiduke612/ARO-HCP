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

package coreinformers

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/Azure/ARO-HCP/internal/api/coreapi"
	"github.com/Azure/ARO-HCP/internal/apitesting/coreapitesting"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/billingcosmosstorage"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/cosmosstorageutils"
	"github.com/Azure/ARO-HCP/internal/utils"
)

type snapshotTestLister[T any] struct {
	item  *T
	calls *atomic.Int32
}

func (l snapshotTestLister[T]) List(context.Context, *cosmosstorageutils.DBClientListResourceDocsOptions) (cosmosstorageutils.DBClientIterator[T], error) {
	l.calls.Add(1)
	return l, nil
}
func (l snapshotTestLister[T]) Items(context.Context) cosmosstorageutils.DBClientIteratorItem[T] {
	return func(yield func(string, *T) bool) { yield("", l.item) }
}
func (snapshotTestLister[T]) GetContinuationToken() string { return "" }
func (snapshotTestLister[T]) GetError() error              { return nil }

type versionedBillingTestLister struct {
	current atomic.Pointer[billingcosmosstorage.BillingDocument]
	calls   atomic.Int32
}

func (l *versionedBillingTestLister) List(context.Context, *cosmosstorageutils.DBClientListResourceDocsOptions) (cosmosstorageutils.DBClientIterator[billingcosmosstorage.BillingDocument], error) {
	l.calls.Add(1)
	return snapshotTestLister[billingcosmosstorage.BillingDocument]{item: l.current.Load(), calls: &l.calls}, nil
}

func TestBillingInformerVersionedUpdates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resync time.Duration
	}{
		{name: "hourly resync", resync: time.Hour},
		{name: "resync disabled", resync: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(utils.ContextWithLogger(t.Context(), testr.New(t)), 15*time.Second)
			defer cancel()
			initial := billingcosmosstorage.NewBillingDocument("CLUSTER-UID", mustParseResourceID(t, coreapitesting.TestClusterResourceID))
			initial.CosmosETag = azcore.ETag(`"version-1"`)
			initial.ManagedResourceGroup = "old-rg"
			lister := &versionedBillingTestLister{}
			lister.current.Store(initial)
			informer := NewBillingInformerWithRelistDuration(lister, 100*time.Millisecond)
			updates := make(chan [2]string, 10)
			registration, err := informer.AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
				UpdateFunc: func(oldObj, newObj any) {
					oldDoc := oldObj.(*billingcosmosstorage.BillingDocument)
					newDoc := newObj.(*billingcosmosstorage.BillingDocument)
					select {
					case updates <- [2]string{oldDoc.GetObjectMeta().GetResourceVersion(), newDoc.GetObjectMeta().GetResourceVersion()}:
					case <-ctx.Done():
					}
				},
			}, cache.HandlerOptions{ResyncPeriod: ptr.To(tc.resync)})
			require.NoError(t, err)
			done := make(chan struct{})
			go func() { defer close(done); informer.RunWithContext(ctx) }()
			defer func() { cancel(); <-done }()
			require.True(t, cache.WaitForCacheSync(ctx.Done(), registration.HasSynced))
			require.Equal(t, "cluster-uid", initial.GetObjectMeta().GetName())
			require.Eventually(t, func() bool { return lister.calls.Load() >= 2 }, 10*time.Second, 20*time.Millisecond)
			changed := initial.DeepCopy()
			changed.CosmosETag = azcore.ETag(`"version-2"`)
			changed.ManagedResourceGroup = "managed-rg"
			lister.current.Store(changed)
			awaitChanged := func() [2]string {
				for {
					select {
					case got := <-updates:
						if got[1] == `"version-2"` {
							return got
						}
					case <-ctx.Done():
						t.Fatal("changed billing document did not notify listener before resync")
					}
				}
			}
			if diff := cmp.Diff([2]string{`"version-1"`, `"version-2"`}, awaitChanged()); diff != "" {
				t.Fatalf("resource versions (-want +got):\n%s", diff)
			}
			cached, exists, err := informer.GetIndexer().GetByKey("cluster-uid")
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "managed-rg", cached.(*billingcosmosstorage.BillingDocument).ManagedResourceGroup)
		})
	}
}

func TestPollingInformerSnapshots(t *testing.T) {
	for _, container := range []string{"billing", "resources"} {
		t.Run(container, func(t *testing.T) {
			resourceID := mustParseResourceID(t, coreapitesting.TestClusterResourceID)
			var calls atomic.Int32
			var informer cache.SharedIndexInformer
			if container == "billing" {
				doc := billingcosmosstorage.NewBillingDocument("cluster-uid", resourceID)
				informer = NewBillingInformerWithRelistDuration(snapshotTestLister[billingcosmosstorage.BillingDocument]{doc, &calls}, time.Hour)
			} else {
				resourceID = mustParseResourceID(t, resourceID.String()+"/managementClusterContents/default")
				doc := &coreapi.ManagementClusterContent{}
				doc.ResourceID = resourceID
				doc.PartitionKey = strings.ToLower(resourceID.SubscriptionID)
				informer = NewManagementClusterContentInformerWithRelistDuration(snapshotTestLister[coreapi.ManagementClusterContent]{doc, &calls}, time.Hour)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			snapshots := make(chan string, 4)
			logger := funcr.NewJSON(func(line string) {
				if strings.Contains(line, `"snapshotType":"cosmos"`) {
					select {
					case snapshots <- line:
					case <-ctx.Done():
					}
				}
			}, funcr.Options{}).WithValues("request_id", "parent-request")
			ctx = utils.ContextWithLogger(ctx, logger)
			done := make(chan struct{})
			go func() { defer close(done); informer.RunWithContext(ctx) }()
			defer func() { cancel(); <-done }()
			require.True(t, cache.WaitForCacheSync(ctx.Done(), informer.HasSynced))
			var entry map[string]any
			select {
			case line := <-snapshots:
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
			case <-ctx.Done():
				t.Fatal("polling informer did not emit a Cosmos snapshot")
			}
			require.Equal(t, "parent-request", entry["request_id"])
			require.Equal(t, resourceID.String(), entry["currentResourceID"])
			require.Equal(t, strings.ToLower(resourceID.SubscriptionID), entry["subscription_id"])
			require.Equal(t, container, entry["objectMetadata"].(map[string]any)["cosmosContainer"])
			content := entry["content"].(map[string]any)
			if container == "billing" {
				require.Equal(t, "cluster-uid", content["id"])
				require.Equal(t, resourceID.String(), content["resourceId"])
			} else {
				require.Equal(t, resourceID.String(), content["resourceID"])
				require.Contains(t, content, "properties")
			}
			require.EqualValues(t, 1, calls.Load(), "logging must not perform a second list")
		})
	}
}
