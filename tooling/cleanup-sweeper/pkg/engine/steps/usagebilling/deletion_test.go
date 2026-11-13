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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armlocks"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/engine/runner"
	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/prerequisites"
)

type credential struct{}

func (credential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "test", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type transport func(*http.Request) (*http.Response, error)

func (f transport) Do(r *http.Request) (*http.Response, error) { return f(r) }

type finalStep struct{ called bool }

func (*finalStep) Name() string          { return "RG deletion sentinel" }
func (*finalStep) RetryLimit() int       { return 1 }
func (*finalStep) ContinueOnError() bool { return false }
func (s *finalStep) Discover(context.Context) ([]runner.Target, error) {
	s.called = true
	return nil, nil
}
func (*finalStep) Delete(context.Context, runner.Target, bool) error { return nil }
func (*finalStep) Verify(context.Context) error                      { return nil }

func TestOrderedAUBCleanup(t *testing.T) {
	const group = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/billing"
	const account = group + "/providers/Microsoft.UsageBilling/accounts/test"
	const pipeline = account + "/pipelines/Basic"
	const selector = pipeline + "/outputSelectors/default"
	const output = account + "/pav2outputs/default"
	const input = account + "/inputs/default"
	const export = account + "/dataExports/default"
	const metric = account + "/metricExports/default"
	const producer = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/service/providers/Microsoft.ContainerService/managedClusters/service"
	allWrites := []string{"PUT " + selector, "DELETE " + selector, "DELETE " + output, "DELETE " + pipeline, "DELETE " + input, "DELETE " + export, "DELETE " + metric, "DELETE " + account}
	for _, tc := range []struct {
		name                  string
		dryRun                bool
		locked                bool
		lockFailure           bool
		pagination            bool
		pageFailure           bool
		foreignChild          bool
		missingCollection     bool
		deleteFailure         bool
		pollFailure           bool
		async                 bool
		softDelete            bool
		absent                bool
		resume                bool
		producerStatus        int
		producerDeleting      bool
		missingProducer       bool
		unrelatedPrerequisite bool
		wrongProducerType     bool
		want                  []string
		wantError             string
		wantFinal             bool
	}{
		{name: "ordered wait even when workflow wait false", want: allWrites, wantFinal: true},
		{name: "async operations awaited", async: true, want: allWrites, wantFinal: true},
		{name: "dry run", dryRun: true, wantFinal: true},
		{name: "locked", locked: true, wantError: "management locks"},
		{name: "lock read fails", lockFailure: true, wantError: "list locks"},
		{name: "paginated children", pagination: true, want: allWrites, wantFinal: true},
		{name: "late page failure blocks writes", pagination: true, pageFailure: true, wantError: "list "},
		{name: "foreign child blocks writes", foreignChild: true, wantError: "unexpected resource"},
		{name: "missing collection payload blocks writes", missingCollection: true, wantError: "missing its value array"},
		{name: "delete fails blocks RG", deleteFailure: true, want: allWrites[:3], wantError: "delete "},
		{name: "poll fails blocks RG", async: true, pollFailure: true, want: allWrites[:2], wantError: "wait for deletion"},
		{name: "soft deleted resource blocks RG", softDelete: true, want: allWrites[:2], wantError: "remains after deletion"},
		{name: "absent account", absent: true, wantFinal: true},
		{name: "resume after selector and output deletion", resume: true, want: allWrites[3:], wantFinal: true},
		{name: "producer exists", producerStatus: 200, wantError: "still exists"},
		{name: "producer deleting", producerStatus: 200, producerDeleting: true, wantError: "still exists"},
		{name: "producer access denied", producerStatus: 403, wantError: "verify absence prerequisite"},
		{name: "producer unconfigured", missingProducer: true, wantError: "requires producer"},
		{name: "generic prerequisite is not a producer", unrelatedPrerequisite: true, wantError: "requires producer"},
		{name: "producer must be AKS", wrongProducerType: true, wantError: "must identify an AKS cluster"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleted := map[string]bool{}
			closed := false
			var writes []string
			reply := func(r *http.Request, code int, value any) (*http.Response, error) {
				body, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
			}
			clientOptions := &azcorearm.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{MaxRetries: -1}, Transport: transport(func(r *http.Request) (*http.Response, error) {
				path := r.URL.Path
				if path == producer {
					if tc.producerStatus == 200 {
						state := "Succeeded"
						if tc.producerDeleting {
							state = "Deleting"
						}
						return reply(r, 200, map[string]any{"id": producer, "properties": map[string]string{"provisioningState": state}})
					}
					if tc.producerStatus != 0 {
						return reply(r, tc.producerStatus, map[string]any{"error": map[string]string{"code": "Forbidden"}})
					}
					return reply(r, 404, map[string]any{"error": map[string]string{"code": "ResourceNotFound"}})
				}
				if strings.Contains(path, "/providers/Microsoft.Authorization/locks") {
					if tc.lockFailure {
						return reply(r, 403, map[string]any{"error": map[string]string{"code": "Forbidden"}})
					}
					items := []any{}
					if tc.locked {
						items = append(items, map[string]string{"id": "lock"})
					}
					return reply(r, 200, map[string]any{"value": items})
				}
				if path == "/poll" {
					if tc.pollFailure {
						return reply(r, 200, map[string]any{"status": "Failed", "error": map[string]string{"code": "Failure"}})
					}
					return reply(r, 200, map[string]string{"status": "Succeeded"})
				}
				if path == group+"/resources" {
					items := []any{}
					if !tc.absent && !deleted[account] {
						items = append(items, map[string]string{"id": account, "name": "test", "type": accountType})
					}
					return reply(r, 200, map[string]any{"value": items})
				}
				collections := map[string]string{
					account + "/pipelines": pipeline, pipeline + "/outputSelectors": selector,
					account + "/pav2outputs": output, account + "/inputs": input,
					account + "/dataExports": export, account + "/metricExports": metric,
				}
				if id, ok := collections[path]; ok {
					if tc.missingCollection {
						return reply(r, 200, map[string]any{})
					}
					if tc.pagination && path == account+"/pipelines" && len(r.URL.Query().Get("page")) == 0 {
						return reply(r, 200, map[string]any{"value": []any{}, "nextLink": "https://management.azure.com" + path + "?api-version=" + apiVersion + "&page=2"})
					}
					if tc.pageFailure && r.URL.Query().Get("page") == "2" {
						return reply(r, 403, map[string]any{"error": map[string]string{"code": "Forbidden"}})
					}
					items := []any{}
					if !deleted[id] && !(tc.resume && (id == selector || id == output)) {
						childID := id
						if tc.foreignChild {
							childID = "/subscriptions/other/resourceGroups/foreign/providers/Microsoft.UsageBilling/accounts/foreign"
						}
						items = append(items, map[string]string{"id": childID})
					}
					return reply(r, 200, map[string]any{"value": items})
				}
				if !slices.Contains([]string{account, pipeline, selector, output, input, export, metric}, path) {
					return nil, fmt.Errorf("unexpected request %s", r.URL)
				}
				if r.Method == http.MethodGet {
					if deleted[path] {
						return reply(r, 404, map[string]any{"error": map[string]string{"code": "ResourceNotFound"}})
					}
					end := "9999-12-31T23:59:59Z"
					if closed {
						end = "2026-01-01T00:00:00Z"
					}
					return reply(r, 200, map[string]any{"id": path, "properties": map[string]any{"provisioningState": "Succeeded", "startDate": "2025-01-01T00:00:00Z", "endDate": end, "outputId": "pav2outputs"}})
				}
				writes = append(writes, r.Method+" "+path)
				if r.Method == http.MethodPut {
					var body struct {
						Properties map[string]any `json:"properties"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						return nil, err
					}
					if body.Properties["outputId"] != "/pav2outputs/default" {
						return nil, errors.New("invalid outputId request representation")
					}
					closed = true
					return reply(r, 200, map[string]any{"id": path, "properties": map[string]string{"provisioningState": "Succeeded"}})
				}
				if tc.deleteFailure && path == output {
					return reply(r, 409, map[string]any{"error": map[string]string{"code": "Conflict"}})
				}
				if !tc.softDelete {
					deleted[path] = true
				}
				if tc.async {
					rsp, err := reply(r, 202, nil)
					if err != nil {
						return nil, err
					}
					rsp.Header.Set("Azure-AsyncOperation", "https://management.azure.com/poll")
					return rsp, nil
				}
				return reply(r, 204, nil)
			})}}
			resources, err := armresources.NewClient("11111111-1111-1111-1111-111111111111", credential{}, clientOptions)
			if err != nil {
				t.Fatal(err)
			}
			locks, err := armlocks.NewManagementLocksClient("11111111-1111-1111-1111-111111111111", credential{}, clientOptions)
			if err != nil {
				t.Fatal(err)
			}
			var requireAbsent []prerequisites.ResourceAbsence
			if !tc.missingProducer {
				purpose := prerequisites.Producer
				if tc.unrelatedPrerequisite {
					purpose = ""
				}
				id := producer
				if tc.wrongProducerType {
					id = account
				}
				requireAbsent = []prerequisites.ResourceAbsence{{ID: id, APIVersion: "2024-10-01", Purpose: purpose}}
			}
			step, err := NewDeletionStep("billing", resources, locks, credential{}, clientOptions, requireAbsent)
			if err != nil {
				t.Fatal(err)
			}
			last := &finalStep{}
			workflow := runner.Engine{Steps: []runner.Step{step, last}, Parallelism: 1, DryRun: tc.dryRun, Wait: false}
			err = workflow.Run(logr.NewContext(context.Background(), logr.Discard()))
			if len(tc.wantError) == 0 && err != nil {
				t.Fatal(err)
			}
			if len(tc.wantError) != 0 && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("want %q, got %v", tc.wantError, err)
			}
			if diff := cmp.Diff(tc.want, writes); diff != "" {
				t.Fatalf("writes (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantFinal, last.called); diff != "" {
				t.Fatalf("RG deletion reached (-want +got):\n%s", diff)
			}
		})
	}
}

func TestClosedSelector(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		properties map[string]any
		want       map[string]any
		wantError  string
	}{
		{name: "GET output type becomes PUT path", properties: map[string]any{"startDate": "2025-01-01T00:00:00Z", "outputId": "pav2outputs"}, want: map[string]any{"startDate": "2025-01-01T00:00:00Z", "endDate": "2026-11-12T00:00:00Z", "outputId": "/pav2outputs/default"}},
		{name: "already closed", properties: map[string]any{"startDate": "2025-01-01T00:00:00Z", "endDate": "2026-01-01T00:00:00Z", "outputId": "/pav2outputs/default"}},
		{name: "future start", properties: map[string]any{"startDate": "2030-01-01T00:00:00Z"}, wantError: "past timestamp"},
		{name: "category settings", properties: map[string]any{"categorySettings": []any{}}, wantError: "category-based"},
		{name: "invalid end date", properties: map[string]any{"startDate": "2025-01-01T00:00:00Z", "endDate": "bad", "outputId": "pav2outputs"}, wantError: "invalid selector endDate"},
		{name: "other output", properties: map[string]any{"startDate": "2025-01-01T00:00:00Z", "outputId": "streamoutputs"}, wantError: "unsupported selector output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, err := json.Marshal(tc.properties)
			if err != nil {
				t.Fatal(err)
			}
			got, err := closedSelector(tc.properties, now)
			if len(tc.wantError) == 0 && err != nil {
				t.Fatal(err)
			}
			if len(tc.wantError) != 0 && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
				t.Fatalf("want %q, got %v", tc.wantError, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("selector (-want +got):\n%s", diff)
			}
			after, err := json.Marshal(tc.properties)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, after); diff != "" {
				t.Fatalf("input changed (-want +got):\n%s", diff)
			}
		})
	}
}
