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
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"

	"github.com/Azure/ARO-HCP/backend/pkg/utils/controllerutils"
	"github.com/Azure/ARO-HCP/internal/api/coreapi"
	"github.com/Azure/ARO-HCP/internal/apihelpers/coreapihelpers"
	controllerutil "github.com/Azure/ARO-HCP/internal/controllerutils"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/billingcosmosstorage"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/corecosmosstorage"
	"github.com/Azure/ARO-HCP/internal/database/cosmosstorage/cosmosstorageutils"
	"github.com/Azure/ARO-HCP/internal/database/listers/corelisters"
	"github.com/Azure/ARO-HCP/internal/utils"
)

const ReconcileBillingManagedResourceGroupControllerName = "ReconcileBillingManagedResourceGroup"

// billingMetadataInconsistency is diagnostic data, not a reconciliation failure:
// billing metadata must not affect the cluster's aggregated Degraded condition.
type billingMetadataInconsistency struct {
	reason  string
	message string
}

func (e *billingMetadataInconsistency) Error() string { return e.message }

type reconcileBillingManagedResourceGroup struct {
	clusterLister     corelisters.ClusterLister
	billingLister     corelisters.BillingLister
	resourcesDBClient corecosmosstorage.ResourcesDBClient
	billingDBClient   billingcosmosstorage.BillingDBClient
}

// NewReconcileBillingManagedResourceGroupController repairs the managed RG on
// existing billing documents. In particular, older writers used the customer's
// cluster RG instead. Linking and creating billing documents remain separate work.
func NewReconcileBillingManagedResourceGroupController(resourcesDBClient corecosmosstorage.ResourcesDBClient, billingDBClient billingcosmosstorage.BillingDBClient, clusterLister corelisters.ClusterLister, billingLister corelisters.BillingLister) controllerutil.GenericSyncer[controllerutils.HCPClusterKey] {
	return &reconcileBillingManagedResourceGroup{
		clusterLister:     clusterLister,
		billingLister:     billingLister,
		resourcesDBClient: resourcesDBClient,
		billingDBClient:   billingDBClient,
	}
}

// Billing repair must retry storage failures without writing per-cluster health
// conditions, so it uses the generic worker rather than the cluster-status wrapper.
func newManagedRGWatchingController(syncer controllerutil.GenericSyncer[controllerutils.HCPClusterKey], clusterInformer, billingInformer controllerutil.Notifier) (*controllerutil.GenericWatchingController[controllerutils.HCPClusterKey], error) {
	controller := controllerutil.NewGenericWatchingController(ReconcileBillingManagedResourceGroupControllerName, coreapi.ClusterResourceType, syncer, controllerutils.ReconcileTotal)
	const resync = 60 * time.Minute
	if err := controller.QueueForInformersWithMaxDepth(resync, 0, clusterInformer); err != nil {
		return nil, fmt.Errorf("register cluster watch: %w", err)
	}
	// Billing documents carry the owning cluster's ARM ID rather than implementing
	// CosmosPersistable, which the generic Cosmos event handler requires.
	enqueueBilling := func(obj any) {
		doc := obj.(*billingcosmosstorage.BillingDocument)
		if doc != nil {
			controller.EnqueueResourceIDAddWithMaxDepth(doc.ResourceID, true, 0)
		}
	}
	logger := utils.DefaultLogger().WithValues(utils.LogValues{}.AddControllerName(ReconcileBillingManagedResourceGroupControllerName)...)
	_, err := billingInformer.AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: enqueueBilling,
		UpdateFunc: func(oldObj, newObj any) {
			// Recheck both owners if a document's resource ID is corrected.
			enqueueBilling(oldObj)
			enqueueBilling(newObj)
		},
	}, cache.HandlerOptions{Logger: &logger, ResyncPeriod: ptr.To(resync)})
	if err != nil {
		return nil, fmt.Errorf("register billing watch: %w", err)
	}
	return controller, nil
}

func (c *reconcileBillingManagedResourceGroup) CooldownChecker() controllerutil.CooldownChecker {
	return nil
}

func (c *reconcileBillingManagedResourceGroup) MakeKey(resourceID *azcorearm.ResourceID) controllerutils.HCPClusterKey {
	return controllerutils.HCPClusterKey{
		SubscriptionID:    resourceID.SubscriptionID,
		ResourceGroupName: resourceID.ResourceGroupName,
		HCPClusterName:    resourceID.Name,
	}
}

func (c *reconcileBillingManagedResourceGroup) NeedsWork(cluster *coreapi.Cluster) bool {
	return cluster != nil && cluster.ID != nil &&
		cluster.ServiceProviderProperties.DeletionTimestamp == nil &&
		cluster.ServiceProviderProperties.ProvisioningState != coreapi.ProvisioningStateDeleting &&
		len(cluster.ServiceProviderProperties.BillingDocumentCosmosID) > 0
}

func (c *reconcileBillingManagedResourceGroup) SyncOnce(ctx context.Context, key controllerutils.HCPClusterKey) error {
	cluster, err := c.clusterLister.Get(ctx, key.SubscriptionID, key.ResourceGroupName, key.HCPClusterName)
	if cosmosstorageutils.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}
	if !c.NeedsWork(cluster) {
		return nil
	}
	doc, err := c.billingLister.GetByID(ctx, cluster.ServiceProviderProperties.BillingDocumentCosmosID)
	if err != nil && !cosmosstorageutils.IsNotFoundError(err) {
		return utils.TrackError(err)
	}
	// Only skip the live reads when the cache already proves convergence. A
	// missing or mismatched cached document can reflect independently lagging caches.
	if err == nil && doc != nil {
		if desired, err := billingManagedResourceGroup(cluster, doc); err == nil && desired != "" && managedResourceGroupsEqual(doc.ManagedResourceGroup, desired) {
			return nil
		}
	}

	liveCluster, err := c.resourcesDBClient.HCPClusters(key.SubscriptionID, key.ResourceGroupName).Get(ctx, key.HCPClusterName)
	if cosmosstorageutils.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}
	if !c.NeedsWork(liveCluster) {
		return nil
	}
	billingCRUD := c.billingDBClient.BillingDocs(liveCluster.ID.SubscriptionID)
	liveDoc, err := billingCRUD.GetByID(ctx, liveCluster.ServiceProviderProperties.BillingDocumentCosmosID)
	if cosmosstorageutils.IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return utils.TrackError(err)
	}
	desired, inconsistency := billingManagedResourceGroup(liveCluster, liveDoc)
	if inconsistency != nil {
		key.AddLoggerValues(utils.LoggerFromContext(ctx)).Error(inconsistency, "billing metadata requires reconciliation",
			"event", "billing_metadata_inconsistency",
			"reason", inconsistency.reason,
			"billingDocID", liveDoc.ID,
			"clusterUID", liveCluster.ServiceProviderProperties.ClusterUID,
			"billingDocumentCosmosID", liveCluster.ServiceProviderProperties.BillingDocumentCosmosID)
		// Input events and resync recheck persistent data inconsistencies without
		// treating them as transient storage failures in the retry loop.
		return nil
	}
	if desired == "" || managedResourceGroupsEqual(liveDoc.ManagedResourceGroup, desired) {
		return nil
	}
	if liveDoc.CosmosETag == "" {
		return fmt.Errorf("billing document %q has no ETag; refusing managed resource group repair", liveDoc.ID)
	}
	etagLiteral, err := json.Marshal(string(liveDoc.CosmosETag))
	if err != nil {
		return fmt.Errorf("encode billing document ETag: %w", err)
	}

	var patch billingcosmosstorage.BillingDocumentPatchOperations
	previous := liveDoc.ManagedResourceGroup
	patch.SetManagedResourceGroup(desired)
	// A conditional PATCH atomically checks the version used for the identity
	// decision, while preserving fields owned by other writers. Parenthesize the
	// active predicate so explicit null cannot bypass the version check.
	condition := fmt.Sprintf("FROM c WHERE c._etag = %s AND (NOT IS_DEFINED(c.deletionTime) OR IS_NULL(c.deletionTime))", etagLiteral)
	// azcosmos v1.4 interpolates the condition into a JSON string without
	// escaping it. Encode that string layer explicitly to preserve quoted ETags.
	encodedCondition, err := json.Marshal(condition)
	if err != nil {
		return fmt.Errorf("encode billing patch condition: %w", err)
	}
	patch.SetCondition(string(encodedCondition[1 : len(encodedCondition)-1]))
	if err := billingCRUD.PatchByID(ctx, liveDoc.ID, patch); err != nil {
		if cosmosstorageutils.IsNotFoundError(err) {
			return nil
		}
		// A 412 goes through the rate-limited worker retry, which reads the
		// authoritative documents again instead of replaying this patch.
		return utils.TrackError(err)
	}
	utils.LoggerFromContext(ctx).Info("reconciled billing managed resource group",
		"billingDocID", liveDoc.ID, "previousManagedResourceGroup", previous, "managedResourceGroup", desired)
	return nil
}

// billingManagedResourceGroup validates incarnation identity before using cluster
// metadata. Resource ID alone is insufficient when a cluster name is reused.
func billingManagedResourceGroup(cluster *coreapi.Cluster, doc *billingcosmosstorage.BillingDocument) (string, *billingMetadataInconsistency) {
	if doc.DeletionTime != nil {
		return "", nil
	}
	uid := cluster.ServiceProviderProperties.ClusterUID
	if uid == "" || uid != cluster.ServiceProviderProperties.BillingDocumentCosmosID || doc.ID != uid ||
		doc.ResourceID == nil || !strings.EqualFold(doc.ResourceID.String(), cluster.ID.String()) ||
		!strings.EqualFold(doc.SubscriptionID, cluster.ID.SubscriptionID) {
		return "", &billingMetadataInconsistency{
			reason:  "ClusterIdentityMismatch",
			message: fmt.Sprintf("billing document %q does not match cluster incarnation %q", doc.ID, uid),
		}
	}
	name := cluster.CustomerProperties.Platform.ManagedResourceGroup
	if len(strings.TrimSpace(name)) == 0 {
		return "", &billingMetadataInconsistency{
			reason:  "MissingManagedResourceGroup",
			message: fmt.Sprintf("cluster has no configured managed resource group for billing document %q", doc.ID),
		}
	}
	// Do not use the customer's RG as a fallback: that is the historical defect
	// this controller repairs. Only a configured RG name is authoritative here.
	return coreapihelpers.ToResourceGroupResourceIDString(cluster.ID.SubscriptionID, name), nil
}

func managedResourceGroupsEqual(a, b string) bool {
	return strings.EqualFold(strings.TrimRight(a, "/"), strings.TrimRight(b, "/"))
}
