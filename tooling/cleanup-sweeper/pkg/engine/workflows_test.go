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

package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azcorearm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Azure/ARO-HCP/tooling/cleanup-sweeper/pkg/engine/runner"
)

type workflowTransport func(*http.Request) (*http.Response, error)

func (f workflowTransport) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestAUBPrecedesResourceGroupCleanup(t *testing.T) {
	for _, bestEffort := range []bool{false, true} {
		t.Run(fmt.Sprint("continueOnError=", bestEffort), func(t *testing.T) {
			options := &azcorearm.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: workflowTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Request: r, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"name":"billing","location":"westus3"}`))}, nil
			})}}
			workflow, err := ResourceGroupOrderedCleanupWorkflow(logr.NewContext(context.Background(), logr.Discard()), "billing", "11111111-1111-1111-1111-111111111111", workflowsTestCredential{}, WorkflowOptions{ContinueOnError: bestEffort, ClientOptions: options})
			if err != nil {
				t.Fatal(err)
			}
			if workflow.Steps[0].Name() != "Delete Azure Usage Billing accounts" || workflow.Steps[0].ContinueOnError() {
				t.Fatal("AUB must be a strict prerequisite for all subsequent cleanup")
			}
		})
	}
}

func TestWorkflowBuilders(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		execute    func(t *testing.T) (interface{}, error)
		assertions func(t *testing.T, workflow interface{}, err error)
	}{
		{
			name: "role assignments workflow builds shared-leftovers steps",
			execute: func(_ *testing.T) (interface{}, error) {
				return RoleAssignmentsSweeperWorkflow(
					context.Background(),
					"00000000-0000-0000-0000-000000000000",
					workflowsTestCredential{},
					workflowsTestCredential{},
					WorkflowOptions{
						DryRun:      true,
						Wait:        true,
						Parallelism: 7,
					},
				)
			},
			assertions: func(t *testing.T, workflow interface{}, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("expected no error while building workflow, got %v", err)
				}
				builtWorkflow, ok := workflow.(*runner.Engine)
				if !ok || builtWorkflow == nil {
					t.Fatalf("expected *runner.Engine workflow")
				}
				if len(builtWorkflow.Steps) != 2 {
					t.Fatalf("expected two steps, got %d", len(builtWorkflow.Steps))
				}
				if builtWorkflow.Parallelism != 7 || !builtWorkflow.DryRun || !builtWorkflow.Wait {
					t.Fatalf("unexpected workflow options: %+v", builtWorkflow)
				}
			},
		},
		{
			name: "role assignments workflow defaults graph credential to arm credential when nil",
			execute: func(_ *testing.T) (interface{}, error) {
				return RoleAssignmentsSweeperWorkflow(
					context.Background(),
					"00000000-0000-0000-0000-000000000000",
					workflowsTestCredential{},
					nil,
					WorkflowOptions{
						DryRun:      true,
						Wait:        true,
						Parallelism: 7,
					},
				)
			},
			assertions: func(t *testing.T, workflow interface{}, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("expected no error while building workflow with nil graph credential, got %v", err)
				}
				builtWorkflow, ok := workflow.(*runner.Engine)
				if !ok || builtWorkflow == nil {
					t.Fatalf("expected *runner.Engine workflow")
				}
				if len(builtWorkflow.Steps) != 2 {
					t.Fatalf("expected two steps, got %d", len(builtWorkflow.Steps))
				}
			},
		},
		{
			name: "resource group ordered workflow propagates canceled context",
			execute: func(_ *testing.T) (interface{}, error) {
				baseCtx := logr.NewContext(context.Background(), logr.Discard())
				ctx, cancel := context.WithCancel(baseCtx)
				cancel()
				return ResourceGroupOrderedCleanupWorkflow(
					ctx,
					"rg-example",
					"00000000-0000-0000-0000-000000000000",
					workflowsTestCredential{},
					WorkflowOptions{},
				)
			},
			assertions: func(t *testing.T, _ interface{}, err error) {
				t.Helper()
				if err == nil {
					t.Fatalf("expected error when context is canceled")
				}
				if !strings.Contains(err.Error(), "failed to get resource group") {
					t.Fatalf("unexpected error: %v", err)
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			workflow, err := tc.execute(t)
			tc.assertions(t, workflow, err)
		})
	}
}

type workflowsTestCredential struct{}

func (workflowsTestCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}
