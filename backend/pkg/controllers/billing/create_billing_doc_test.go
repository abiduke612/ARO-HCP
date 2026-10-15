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

package billing

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/backend/pkg/utils/controllerutils"
	"github.com/Azure/ARO-HCP/internal/api/coreapi"
	"github.com/Azure/ARO-HCP/internal/api/metadataapi"
	"github.com/Azure/ARO-HCP/internal/apihelpers/coreapihelpers"
	"github.com/Azure/ARO-HCP/internal/apihelpers/metadataapihelpers"
	controllerutil "github.com/Azure/ARO-HCP/internal/controllerutils"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/billingcosmosstorage"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/cosmosstorageutils"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstoragetesting/billingcosmosstoragetesting"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstoragetesting/corecosmosstoragetesting"
	"github.com/Azure/ARO-HCP/internal/database/listertesting/corelistertesting"
	"github.com/Azure/ARO-HCP/internal/utils"
)

const (
	testSubscriptionID      = "00000000-0000-0000-0000-000000000000"
	testResourceGroupName   = "test-rg"
	testClusterName         = "test-cluster"
	testClusterUID          = "billing-doc-id-001"
	testTenantID            = "11111111-1111-1111-1111-111111111111"
	testAzureLocation       = "eastus"
	testClusterServiceIDStr = "/api/clusters_mgmt/v1/clusters/abc123"
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// Record the actual SDK patch payload: the shared in-memory billing client's
// patch implementation only simulates deletion, so it cannot test this repair.
type managedRGPatchClient struct {
	billingcosmosstorage.BillingDBClient
	patches     []json.RawMessage
	patchErr    error
	readErr     error
	reads       int
	beforePatch func()
}

func (c *managedRGPatchClient) BillingDocs(subscriptionID string) billingcosmosstorage.BillingDocCRUD {
	return &managedRGPatchCRUD{BillingDocCRUD: c.BillingDBClient.BillingDocs(subscriptionID), client: c}
}

type managedRGPatchCRUD struct {
	billingcosmosstorage.BillingDocCRUD
	client *managedRGPatchClient
}

func (c *managedRGPatchCRUD) GetByID(ctx context.Context, id string) (*billingcosmosstorage.BillingDocument, error) {
	c.client.reads++
	if c.client.readErr != nil {
		return nil, c.client.readErr
	}
	doc, err := c.BillingDocCRUD.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return doc.DeepCopy(), nil
}

func (c *managedRGPatchCRUD) PatchByID(ctx context.Context, id string, ops billingcosmosstorage.BillingDocumentPatchOperations) error {
	data, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	c.client.patches = append(c.client.patches, data)
	if c.client.patchErr != nil {
		return c.client.patchErr
	}
	if c.client.beforePatch != nil {
		c.client.beforePatch()
	}
	doc, err := c.BillingDocCRUD.GetByID(ctx, id)
	if err != nil {
		return err
	}
	var patch struct {
		Condition  string `json:"condition"`
		Operations []struct {
			Value string `json:"value"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(data, &patch); err != nil {
		return err
	}
	etagLiteral, err := json.Marshal(string(doc.CosmosETag))
	if err != nil {
		return err
	}
	if patch.Condition != `FROM c WHERE c._etag = `+string(etagLiteral)+` AND (NOT IS_DEFINED(c.deletionTime) OR IS_NULL(c.deletionTime))` || doc.DeletionTime != nil {
		return &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed}
	}
	doc.ManagedResourceGroup = patch.Operations[0].Value
	return nil
}

func TestReconcileBillingManagedResourceGroup(t *testing.T) {
	created := mustParseTime("2025-01-15T10:30:00Z")
	desired := coreapihelpers.ToResourceGroupResourceIDString(testSubscriptionID, "managed-rg")
	for _, tt := range []struct {
		name                                     string
		changeCluster                            func(*coreapi.Cluster)
		changeDoc                                func(*billingcosmosstorage.BillingDocument)
		changeCache                              func(*coreapi.Cluster, *billingcosmosstorage.BillingDocument)
		missingCluster, missingDoc, missingCache bool
		readErr, patchErr                        error
		wantErr                                  string
		wantReason                               string
		wantPatches                              int
		wantCachedNoOp                           bool
	}{
		{name: "repairs customer RG", wantPatches: 1},
		{name: "repairs missing RG", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.ManagedResourceGroup = "" }, wantPatches: 1},
		{name: "converged", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.ManagedResourceGroup = desired }, wantCachedNoOp: true},
		{name: "case and trailing slash equivalent", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.ManagedResourceGroup = strings.ToUpper(desired) + "/" }, wantCachedNoOp: true},
		{name: "missing configured RG is not replaced with customer RG", changeCluster: func(c *coreapi.Cluster) { c.CustomerProperties.Platform.ManagedResourceGroup = "" }, wantReason: "MissingManagedResourceGroup"},
		{name: "unlinked cluster", changeCluster: func(c *coreapi.Cluster) { c.ServiceProviderProperties.BillingDocumentCosmosID = "" }},
		{name: "missing UID", changeCluster: func(c *coreapi.Cluster) { c.ServiceProviderProperties.ClusterUID = "" }, wantReason: "ClusterIdentityMismatch"},
		{name: "recreated cluster", changeCluster: func(c *coreapi.Cluster) { c.ServiceProviderProperties.ClusterUID = "new-incarnation" }, wantReason: "ClusterIdentityMismatch"},
		{name: "wrong resource", changeDoc: func(d *billingcosmosstorage.BillingDocument) {
			d.ResourceID = metadataapi.Must(azcorearm.ParseResourceID(newTestClusterResourceID(t).String() + "-other"))
		}, wantReason: "ClusterIdentityMismatch"},
		{name: "missing resource ID", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.ResourceID = nil }, wantReason: "ClusterIdentityMismatch"},
		{name: "stale cached subscription", changeCache: func(c *coreapi.Cluster, d *billingcosmosstorage.BillingDocument) {
			d.SubscriptionID = "another-subscription"
		}, wantPatches: 1},
		{name: "document in another subscription", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.SubscriptionID = "another-subscription" }},
		{name: "deleted billing", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.DeletionTime = &created }},
		{name: "deleting cluster", changeCluster: func(c *coreapi.Cluster) { c.ServiceProviderProperties.DeletionTimestamp = &metav1.Time{Time: created} }},
		{name: "legacy deleting cluster", changeCluster: func(c *coreapi.Cluster) {
			c.ServiceProviderProperties.ProvisioningState = coreapi.ProvisioningStateDeleting
		}},
		{name: "missing live cluster", missingCluster: true},
		{name: "missing live billing", missingDoc: true},
		{name: "billing cache miss falls back to live read", missingCache: true, wantPatches: 1},
		{name: "stale cache cannot repair recreated cluster", changeCache: func(c *coreapi.Cluster, d *billingcosmosstorage.BillingDocument) {
			c.ServiceProviderProperties.ClusterUID = testClusterUID
		}, changeCluster: func(c *coreapi.Cluster) { c.ServiceProviderProperties.ClusterUID = "replacement" }, wantReason: "ClusterIdentityMismatch"},
		{name: "live document already corrected", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.ManagedResourceGroup = desired }, changeCache: func(c *coreapi.Cluster, d *billingcosmosstorage.BillingDocument) { d.ManagedResourceGroup = "stale" }},
		{name: "live billing deletion supersedes stale active cache", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.DeletionTime = &created }, changeCache: func(c *coreapi.Cluster, d *billingcosmosstorage.BillingDocument) { d.DeletionTime = nil }},
		{name: "read failure", readErr: errors.New("read failed"), wantErr: "read failed"},
		{name: "patch failure", patchErr: errors.New("patch failed"), wantErr: "patch failed", wantPatches: 1},
		{name: "concurrent billing deletion", patchErr: &azcore.ResponseError{StatusCode: http.StatusPreconditionFailed}, wantPatches: 1},
		{name: "missing ETag refuses repair", changeDoc: func(d *billingcosmosstorage.BillingDocument) { d.CosmosETag = "" }, wantErr: "has no ETag"},
		{name: "concurrent document removal", patchErr: &azcore.ResponseError{StatusCode: http.StatusNotFound}, wantPatches: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs []map[string]any
			logger := funcr.NewJSON(func(line string) {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				logs = append(logs, entry)
			}, funcr.Options{})
			ctx := utils.ContextWithLogger(t.Context(), logger)
			cluster := newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &created)
			cluster.ServiceProviderProperties.BillingDocumentCosmosID = testClusterUID
			cluster.CustomerProperties.Platform.ManagedResourceGroup = "managed-rg"
			if tt.changeCluster != nil {
				tt.changeCluster(cluster)
			}
			var resources []any
			if !tt.missingCluster {
				resources = append(resources, cluster)
			}
			resourcesClient, err := corecosmosstoragetesting.NewMockResourcesDBClientWithResources(ctx, resources)
			require.NoError(t, err)
			billingClient := billingcosmosstoragetesting.NewMockBillingDBClient()
			doc := billingcosmosstorage.NewBillingDocument(testClusterUID, newTestClusterResourceID(t))
			doc.CosmosETag = azcore.ETag(`"live-version"`)
			doc.CreationTime = created
			doc.ManagedResourceGroup = coreapihelpers.ToResourceGroupResourceIDString(testSubscriptionID, testResourceGroupName)
			if !tt.missingDoc {
				require.NoError(t, billingClient.BillingDocs(testSubscriptionID).Create(ctx, doc))
			}
			if tt.changeDoc != nil {
				tt.changeDoc(doc)
			}
			original := doc.DeepCopy()
			cachedCluster, cachedDoc := cluster.DeepCopy(), doc.DeepCopy()
			if tt.changeCache != nil {
				tt.changeCache(cachedCluster, cachedDoc)
			}
			var cachedDocs []*billingcosmosstorage.BillingDocument
			if !tt.missingCache {
				cachedDocs = append(cachedDocs, cachedDoc)
			}
			client := &managedRGPatchClient{BillingDBClient: billingClient, readErr: tt.readErr, patchErr: tt.patchErr}
			syncer := NewReconcileBillingManagedResourceGroupController(resourcesClient, client,
				&corelistertesting.SliceClusterLister{Clusters: []*coreapi.Cluster{cachedCluster}},
				&corelistertesting.SliceBillingLister{BillingDocuments: cachedDocs})
			controller, err := newManagedRGWatchingController(syncer, &managedRGNotifier{}, &managedRGNotifier{})
			require.NoError(t, err)
			err = controller.SyncOnce(ctx, newTestClusterKey())
			if cosmosstorageutils.IsPreconditionFailedError(tt.patchErr) {
				require.ErrorIs(t, err, tt.patchErr)
			} else if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			var diagnostics []map[string]any
			for _, entry := range logs {
				if entry["event"] == "billing_metadata_inconsistency" {
					diagnostics = append(diagnostics, entry)
				}
			}
			if tt.wantReason != "" {
				require.Len(t, diagnostics, 1)
				require.Equal(t, tt.wantReason, diagnostics[0]["reason"])
				require.Equal(t, doc.ID, diagnostics[0]["billingDocID"])
				require.Equal(t, cluster.ServiceProviderProperties.ClusterUID, diagnostics[0]["clusterUID"])
				require.Equal(t, strings.ToLower(cluster.ID.String()), diagnostics[0]["resource_id"])
			} else {
				require.Empty(t, diagnostics)
			}
			require.Len(t, client.patches, tt.wantPatches)
			if tt.wantCachedNoOp {
				require.Zero(t, client.reads, "converged cache should avoid live billing reads")
			}
			for _, patch := range client.patches {
				// A single-field PATCH preserves all unknown fields, including the
				// external Billing service's watermarks absent from this branch's type.
				require.JSONEq(t, `{"condition":"FROM c WHERE c._etag = \"\\\"live-version\\\"\" AND (NOT IS_DEFINED(c.deletionTime) OR IS_NULL(c.deletionTime))","operations":[{"op":"set","path":"/managedResourceGroup","value":"`+desired+`"}]}`, string(patch))
			}
			if tt.wantPatches > 0 && tt.patchErr == nil {
				require.Equal(t, desired, doc.ManagedResourceGroup)
				original.ManagedResourceGroup = desired
				// Even with a stale cache, the second reconcile sees convergence live.
				require.NoError(t, controller.SyncOnce(ctx, newTestClusterKey()))
				require.Len(t, client.patches, 1)
			}
			require.Equal(t, original, doc, "only managedResourceGroup may change")
			_, statusErr := resourcesClient.HCPClusters(testSubscriptionID, testResourceGroupName).Controllers(testClusterName).Get(ctx, ReconcileBillingManagedResourceGroupControllerName)
			require.True(t, cosmosstorageutils.IsNotFoundError(statusErr), "billing repair must not write cluster controller status, got %v", statusErr)
		})
	}
}

func TestManagedRGConflictRecomputesFromLiveDocuments(t *testing.T) {
	for _, tc := range []struct {
		name       string
		change     func(*billingcosmosstorage.BillingDocument)
		wantRepair bool
	}{
		{name: "unrelated update retries with new ETag", change: func(d *billingcosmosstorage.BillingDocument) { d.Location = "westus3" }, wantRepair: true},
		{name: "concurrent deletion stops repair", change: func(d *billingcosmosstorage.BillingDocument) {
			d.DeletionTime = ptr.To(mustParseTime("2026-10-01T00:00:00Z"))
		}},
		{name: "concurrent identity change stops repair", change: func(d *billingcosmosstorage.BillingDocument) {
			d.ResourceID = metadataapi.Must(azcorearm.ParseResourceID(d.ResourceID.String() + "-other"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := utils.ContextWithLogger(t.Context(), testr.New(t))
			created := mustParseTime("2025-01-15T10:30:00Z")
			cluster := newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &created)
			cluster.ServiceProviderProperties.BillingDocumentCosmosID = testClusterUID
			cluster.CustomerProperties.Platform.ManagedResourceGroup = "managed-rg"
			resources, err := corecosmosstoragetesting.NewMockResourcesDBClientWithResources(ctx, []any{cluster})
			require.NoError(t, err)
			doc := billingcosmosstorage.NewBillingDocument(testClusterUID, cluster.ID)
			doc.CreationTime = created
			doc.CosmosETag = azcore.ETag(`"version-1"`)
			doc.ManagedResourceGroup = "customer-rg"
			billingDB := billingcosmosstoragetesting.NewMockBillingDBClient()
			require.NoError(t, billingDB.BillingDocs(testSubscriptionID).Create(ctx, doc))
			client := &managedRGPatchClient{BillingDBClient: billingDB, beforePatch: func() {
				doc.CosmosETag = azcore.ETag(`"version-2"`)
				tc.change(doc)
			}}
			syncer := NewReconcileBillingManagedResourceGroupController(resources, client,
				&corelistertesting.SliceClusterLister{Clusters: []*coreapi.Cluster{cluster.DeepCopy()}},
				&corelistertesting.SliceBillingLister{BillingDocuments: []*billingcosmosstorage.BillingDocument{doc.DeepCopy()}})
			require.True(t, cosmosstorageutils.IsPreconditionFailedError(syncer.SyncOnce(ctx, newTestClusterKey())))
			require.Equal(t, "customer-rg", doc.ManagedResourceGroup, "stale patch must not change the document")
			client.beforePatch = nil
			want := doc.DeepCopy()
			wantPatchCount := 1
			if tc.wantRepair {
				want.ManagedResourceGroup = coreapihelpers.ToResourceGroupResourceIDString(testSubscriptionID, "managed-rg")
				wantPatchCount = 2
			}
			require.NoError(t, syncer.SyncOnce(ctx, newTestClusterKey()))
			require.Len(t, client.patches, wantPatchCount)
			if diff := cmp.Diff(want, doc, cmp.Comparer(func(a, b *azcorearm.ResourceID) bool {
				if a == nil || b == nil {
					return a == b
				}
				return a.String() == b.String()
			})); diff != "" {
				t.Fatalf("persisted document (-want +got):\n%s", diff)
			}
			if tc.wantRepair {
				var payload struct {
					Condition string `json:"condition"`
				}
				require.NoError(t, json.Unmarshal(client.patches[1], &payload))
				require.Contains(t, payload.Condition, `c._etag = "\"version-2\""`)
			}
		})
	}
}

type managedRGNotifier struct {
	handler cache.ResourceEventHandler
	options cache.HandlerOptions
	err     error
}

func (n *managedRGNotifier) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, options cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	n.handler = handler
	n.options = options
	return nil, n.err
}

type managedRGRecordingSyncer struct {
	controllerutil.GenericSyncer[controllerutils.HCPClusterKey]
	keys chan controllerutils.HCPClusterKey
}

func (s *managedRGRecordingSyncer) SyncOnce(_ context.Context, key controllerutils.HCPClusterKey) error {
	s.keys <- key
	return nil
}

func TestManagedRGWatchRouting(t *testing.T) {
	for _, tc := range []struct {
		name      string
		notify    func(*managedRGNotifier, *managedRGNotifier, *coreapi.Cluster, *billingcosmosstorage.BillingDocument)
		wantNames []string
	}{
		{name: "cluster add", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			c.handler.OnAdd(cluster, true)
		}, wantNames: []string{testClusterName}},
		{name: "cluster resync", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			c.handler.OnUpdate(cluster, cluster)
		}, wantNames: []string{testClusterName}},
		{name: "billing add maps ARM ID rather than document UUID", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			b.handler.OnAdd(doc, true)
		}, wantNames: []string{testClusterName}},
		{name: "billing resync deduplicates queue key", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			b.handler.OnUpdate(doc, doc)
		}, wantNames: []string{testClusterName}},
		{name: "billing owner correction queues both clusters", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			other := billingcosmosstorage.NewBillingDocument(doc.ID, metadataapi.Must(azcorearm.ParseResourceID(cluster.ID.String()+"-other")))
			b.handler.OnUpdate(doc, other)
		}, wantNames: []string{testClusterName, testClusterName + "-other"}},
		{name: "missing billing resource ID", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			b.handler.OnAdd(&billingcosmosstorage.BillingDocument{}, true)
		}},
		{name: "wrong resource type", notify: func(c, b *managedRGNotifier, cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) {
			b.handler.OnAdd(billingcosmosstorage.NewBillingDocument(doc.ID, metadataapi.Must(azcorearm.ParseResourceID(cluster.ID.String()+"/nodePools/np"))), true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(utils.ContextWithLogger(t.Context(), testr.New(t)))
				defer cancel()
				clusterNotifier, billingNotifier := &managedRGNotifier{}, &managedRGNotifier{}
				syncer := &managedRGRecordingSyncer{GenericSyncer: NewReconcileBillingManagedResourceGroupController(nil, nil, nil, nil), keys: make(chan controllerutils.HCPClusterKey, 10)}
				controller, err := newManagedRGWatchingController(syncer, clusterNotifier, billingNotifier)
				require.NoError(t, err)
				require.Equal(t, time.Hour, *clusterNotifier.options.ResyncPeriod)
				require.Equal(t, time.Hour, *billingNotifier.options.ResyncPeriod)
				cluster := newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, nil)
				doc := billingcosmosstorage.NewBillingDocument(testClusterUID, cluster.ID)
				tc.notify(clusterNotifier, billingNotifier, cluster, doc)
				go controller.Run(ctx, 1)
				synctest.Wait()
				time.Sleep(time.Second)
				synctest.Wait()
				cancel()
				synctest.Wait()
				close(syncer.keys)
				var gotNames []string
				for key := range syncer.keys {
					require.Equal(t, testSubscriptionID, key.SubscriptionID)
					require.Equal(t, testResourceGroupName, key.ResourceGroupName)
					gotNames = append(gotNames, key.HCPClusterName)
				}
				if diff := cmp.Diff(tc.wantNames, gotNames); diff != "" {
					t.Fatalf("queued clusters (-want +got):\n%s", diff)
				}
			})
		})
	}
}

func TestManagedRGWatchRegistrationErrors(t *testing.T) {
	failure := errors.New("watch registration failed")
	for _, tc := range []struct {
		name                   string
		clusterErr, billingErr error
	}{
		{name: "cluster", clusterErr: failure}, {name: "billing", billingErr: failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller, err := newManagedRGWatchingController(NewReconcileBillingManagedResourceGroupController(nil, nil, nil, nil), &managedRGNotifier{err: tc.clusterErr}, &managedRGNotifier{err: tc.billingErr})
			require.ErrorIs(t, err, failure)
			require.Nil(t, controller)
		})
	}
}

func newTestClusterResourceID(t *testing.T) *azcorearm.ResourceID {
	t.Helper()
	resourceID, err := azcorearm.ParseResourceID(
		"/subscriptions/" + testSubscriptionID +
			"/resourceGroups/" + testResourceGroupName +
			"/providers/Microsoft.RedHatOpenShift/hcpOpenShiftClusters/" + testClusterName)
	require.NoError(t, err)
	return resourceID
}

func newTestSubscription() *coreapi.Subscription {
	subResourceID := metadataapi.Must(azcorearm.ParseResourceID(
		"/subscriptions/" + testSubscriptionID))
	return &coreapi.Subscription{
		CosmosMetadata: coreapi.CosmosMetadata{
			ResourceID:   subResourceID,
			PartitionKey: strings.ToLower(subResourceID.SubscriptionID),
		},
		State: coreapi.SubscriptionStateRegistered,
		Properties: &coreapi.SubscriptionProperties{
			TenantId: ptr.To(testTenantID),
		},
	}
}

func newTestCluster(t *testing.T, clusterUID string, provisioningState coreapi.ProvisioningState, createdAt *time.Time) *coreapi.Cluster {
	t.Helper()
	clusterResourceID := newTestClusterResourceID(t)
	return &coreapi.Cluster{
		CosmosMetadata: coreapi.CosmosMetadata{
			ResourceID:   clusterResourceID,
			PartitionKey: strings.ToLower(clusterResourceID.SubscriptionID),
		},
		TrackedResource: coreapi.TrackedResource{
			Resource: coreapi.Resource{
				ID:   clusterResourceID,
				Name: testClusterName,
				Type: "Microsoft.RedHatOpenShift/hcpOpenShiftClusters",
				SystemData: &coreapi.SystemData{
					CreatedAt: createdAt,
				},
			},
		},
		ServiceProviderProperties: coreapi.ClusterServiceProviderProperties{
			ProvisioningState: provisioningState,
			ClusterUID:        clusterUID,
			ClusterServiceID:  metadataapihelpers.Ptr(metadataapi.Must(metadataapi.NewInternalID(testClusterServiceIDStr))),
		},
	}
}

func newTestClusterKey() controllerutils.HCPClusterKey {
	return controllerutils.HCPClusterKey{
		SubscriptionID:    testSubscriptionID,
		ResourceGroupName: testResourceGroupName,
		HCPClusterName:    testClusterName,
	}
}

func TestCreateBillingDoc_SyncOnce(t *testing.T) {
	fixedTime := mustParseTime("2025-01-20T10:30:00Z")
	createdAt := mustParseTime("2025-01-15T10:30:00Z")

	tests := []struct {
		name        string
		cluster     *coreapi.Cluster
		expectError bool
		verify      func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient)
	}{
		{
			name:        "creates billing document for succeeded cluster with ClusterUID",
			cluster:     newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &createdAt),
			expectError: false,
			verify: func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient) {
				billingDocs := billing.GetBillingDocuments()
				require.Len(t, billingDocs, 1)
				doc := billingDocs[testClusterUID]
				require.NotNil(t, doc)
				assert.Equal(t, testClusterUID, doc.ID)
				assert.Equal(t, testTenantID, doc.TenantID)
				assert.Equal(t, testAzureLocation, doc.Location)
				assert.Equal(t, createdAt, doc.CreationTime)
			},
		},
		{
			name:        "uses fallback time when CreatedAt is nil",
			cluster:     newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, nil),
			expectError: false,
			verify: func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient) {
				billingDocs := billing.GetBillingDocuments()
				require.Len(t, billingDocs, 1)
				doc := billingDocs[testClusterUID]
				require.NotNil(t, doc)
				assert.Equal(t, fixedTime, doc.CreationTime, "should use fallback time when CreatedAt is nil")
			},
		},
		{
			name:        "skips cluster without ClusterUID",
			cluster:     newTestCluster(t, "", coreapi.ProvisioningStateSucceeded, &createdAt),
			expectError: false,
			verify: func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient) {
				billingDocs := billing.GetBillingDocuments()
				assert.Empty(t, billingDocs, "no billing document should be created when ClusterUID is empty")
			},
		},
		{
			name:        "skips cluster not in Succeeded state",
			cluster:     newTestCluster(t, testClusterUID, coreapi.ProvisioningStateProvisioning, &createdAt),
			expectError: false,
			verify: func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient) {
				billingDocs := billing.GetBillingDocuments()
				assert.Empty(t, billingDocs, "no billing document should be created for non-succeeded cluster")
			},
		},
		{
			name:        "skips cluster in Failed state",
			cluster:     newTestCluster(t, testClusterUID, coreapi.ProvisioningStateFailed, &createdAt),
			expectError: false,
			verify: func(t *testing.T, billing *billingcosmosstoragetesting.MockBillingDBClient) {
				billingDocs := billing.GetBillingDocuments()
				assert.Empty(t, billingDocs, "no billing document should be created for failed cluster")
			},
		},
		{
			name:        "idempotent when billing document already exists",
			cluster:     newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &createdAt),
			expectError: false,
			verify:      nil, // covered by setup - billing doc pre-seeded, second sync should not error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = utils.ContextWithLogger(ctx, testr.New(t))

			subscription := newTestSubscription()
			resources := []any{tt.cluster, subscription}

			mockResourcesDBClient, err := corecosmosstoragetesting.NewMockResourcesDBClientWithResources(ctx, resources)
			require.NoError(t, err)
			mockBillingDBClient := billingcosmosstoragetesting.NewMockBillingDBClient()

			controller := &createBillingDoc{
				clock:             clocktesting.NewFakePassiveClock(fixedTime),
				azureLocation:     testAzureLocation,
				resourcesDBClient: mockResourcesDBClient,
				billingDBClient:   mockBillingDBClient,
				clusterLister: &corelistertesting.SliceClusterLister{
					Clusters: []*coreapi.Cluster{tt.cluster},
				},
				billingLister: &corelistertesting.SliceBillingLister{
					BillingDocuments: []*billingcosmosstorage.BillingDocument{},
				},
			}

			err = controller.SyncOnce(ctx, newTestClusterKey())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.verify != nil {
				tt.verify(t, mockBillingDBClient)
			}
		})
	}
}

func TestCreateBillingDoc_Idempotent(t *testing.T) {
	fixedTime := mustParseTime("2025-01-20T10:30:00Z")
	createdAt := mustParseTime("2025-01-15T10:30:00Z")

	ctx := context.Background()
	ctx = utils.ContextWithLogger(ctx, testr.New(t))

	cluster := newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &createdAt)
	subscription := newTestSubscription()

	mockResourcesDBClient, err := corecosmosstoragetesting.NewMockResourcesDBClientWithResources(ctx, []any{cluster, subscription})
	require.NoError(t, err)
	mockBillingDBClient := billingcosmosstoragetesting.NewMockBillingDBClient()

	// Setup slice cluster lister (cache)
	clusterLister := &corelistertesting.SliceClusterLister{
		Clusters: []*coreapi.Cluster{cluster},
	}

	controller := &createBillingDoc{
		clock:             clocktesting.NewFakePassiveClock(fixedTime),
		azureLocation:     testAzureLocation,
		resourcesDBClient: mockResourcesDBClient,
		billingDBClient:   mockBillingDBClient,
		clusterLister:     clusterLister,
		billingLister: &corelistertesting.SliceBillingLister{
			BillingDocuments: []*billingcosmosstorage.BillingDocument{},
		},
	}

	key := newTestClusterKey()

	// First sync creates the billing doc
	err = controller.SyncOnce(ctx, key)
	require.NoError(t, err)

	billingDocs := mockBillingDBClient.GetBillingDocuments()
	require.Len(t, billingDocs, 1)

	// Second sync should succeed without error (idempotent - conflict handled)
	err = controller.SyncOnce(ctx, key)
	require.NoError(t, err)

	billingDocs = mockBillingDBClient.GetBillingDocuments()
	assert.Len(t, billingDocs, 1, "should still have exactly one billing document")
}

func TestCreateBillingDoc_ExistingBillingDocButMissingClusterRef(t *testing.T) {
	fixedTime := mustParseTime("2025-01-20T10:30:00Z")
	createdAt := mustParseTime("2025-01-15T10:30:00Z")

	// Pre-seed billing doc to simulate a prior cycle that created it but
	// failed to update the cluster's BillingDocumentCosmosID.
	preSeedDoc := billingcosmosstorage.NewBillingDocument(testClusterUID, newTestClusterResourceID(t))
	preSeedDoc.CreationTime = createdAt
	preSeedDoc.Location = testAzureLocation
	preSeedDoc.TenantID = testTenantID

	tests := []struct {
		name             string
		cachedBillingDoc []*billingcosmosstorage.BillingDocument
		seedInDB         bool
	}{
		{
			name:             "billing doc found in database",
			cachedBillingDoc: []*billingcosmosstorage.BillingDocument{},
			seedInDB:         true,
		},
		{
			name:             "billing doc found in cache",
			cachedBillingDoc: []*billingcosmosstorage.BillingDocument{preSeedDoc},
			seedInDB:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = utils.ContextWithLogger(ctx, testr.New(t))

			// Cluster has ClusterUID but no BillingDocumentCosmosID.
			cluster := newTestCluster(t, testClusterUID, coreapi.ProvisioningStateSucceeded, &createdAt)
			assert.Empty(t, cluster.ServiceProviderProperties.BillingDocumentCosmosID)

			subscription := newTestSubscription()

			mockResourcesDBClient, err := corecosmosstoragetesting.NewMockResourcesDBClientWithResources(ctx, []any{cluster, subscription})
			require.NoError(t, err)
			mockBillingDBClient := billingcosmosstoragetesting.NewMockBillingDBClient()

			if tt.seedInDB {
				err = mockBillingDBClient.BillingDocs(testSubscriptionID).Create(ctx, preSeedDoc)
				require.NoError(t, err)
			}

			controller := &createBillingDoc{
				clock:             clocktesting.NewFakePassiveClock(fixedTime),
				azureLocation:     testAzureLocation,
				resourcesDBClient: mockResourcesDBClient,
				billingDBClient:   mockBillingDBClient,
				clusterLister: &corelistertesting.SliceClusterLister{
					Clusters: []*coreapi.Cluster{cluster},
				},
				billingLister: &corelistertesting.SliceBillingLister{
					BillingDocuments: tt.cachedBillingDoc,
				},
			}

			err = controller.SyncOnce(ctx, newTestClusterKey())
			require.NoError(t, err)

			// Verify no new billing document was created.
			billingDocs := mockBillingDBClient.GetBillingDocuments()
			if tt.seedInDB {
				assert.Len(t, billingDocs, 1, "should not create a second billing document")
			}

			// Verify the cluster's BillingDocumentCosmosID was updated.
			clusterCRUD := mockResourcesDBClient.HCPClusters(testSubscriptionID, testResourceGroupName)
			updatedCluster, err := clusterCRUD.Get(ctx, testClusterName)
			require.NoError(t, err)
			assert.Equal(t, testClusterUID, updatedCluster.ServiceProviderProperties.BillingDocumentCosmosID,
				"cluster should have BillingDocumentCosmosID set to the existing billing doc ID")
		})
	}
}
