// Copyright 2026, Pulumi Corporation.
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

package pulumi

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pulumirpc "github.com/pulumi/pulumi/sdk/v3/proto/go"
)

// hookMonitor is a resource monitor served over gRPC (the mock monitor has no hook support) whose
// answer to SupportsFeature("resourceHooks") and to RegisterResourceHook can be set per test.
type hookMonitor struct {
	pulumirpc.UnimplementedResourceMonitorServer

	supportsHooks bool
	hookRejection error // returned by RegisterResourceHook when not nil

	mu         sync.Mutex
	registered []*pulumirpc.RegisterResourceRequest
}

func (m *hookMonitor) SupportsFeature(
	_ context.Context, req *pulumirpc.SupportsFeatureRequest,
) (*pulumirpc.SupportsFeatureResponse, error) {
	has := req.Id != "outputValues"
	if req.Id == "resourceHooks" {
		has = m.supportsHooks
	}
	return &pulumirpc.SupportsFeatureResponse{HasSupport: has}, nil
}

func (m *hookMonitor) RegisterResourceHook(
	context.Context, *pulumirpc.RegisterResourceHookRequest,
) (*emptypb.Empty, error) {
	if m.hookRejection != nil {
		return nil, m.hookRejection
	}
	return &emptypb.Empty{}, nil
}

func (m *hookMonitor) RegisterResource(
	_ context.Context, req *pulumirpc.RegisterResourceRequest,
) (*pulumirpc.RegisterResourceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registered = append(m.registered, req)
	return &pulumirpc.RegisterResourceResponse{
		Urn: "urn:pulumi:stack::project::" + req.Type + "::" + req.Name, Id: req.Name + "_id", Object: req.Object,
	}, nil
}

func (m *hookMonitor) RegisterResourceOutputs(
	context.Context, *pulumirpc.RegisterResourceOutputsRequest,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *hookMonitor) names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, r := range m.registered {
		out = append(out, r.Name)
	}
	return out
}

// runWithHook runs a program that registers one custom resource bound to a BeforeCreate hook.
func runWithHook(t *testing.T, m *hookMonitor) error {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	pulumirpc.RegisterResourceMonitorServer(srv, m)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	ctx, err := NewContext(context.Background(), RunInfo{
		Project: "project", Stack: "stack", DryRun: true, MonitorAddr: l.Addr().String(),
	})
	require.NoError(t, err)
	defer ctx.Close()

	return RunWithContext(ctx, func(ctx *Context) error {
		hook, err := ctx.RegisterResourceHook("guard", func(*ResourceHookArgs) error { return nil },
			&ResourceHookOptions{OnDryRun: true})
		require.NoError(t, err)
		var res CustomResourceState
		return ctx.RegisterResource("test:index:Thing", "thing", Map{}, &res,
			ResourceHooks(&ResourceHookBinding{BeforeCreate: []*ResourceHook{hook}}))
	})
}

// A hook registration the engine rejects must fail the program. The registration runs
// asynchronously and its error only surfaces when the resource that uses the hook is registered;
// that error used to be dropped there, so the resource was left out and the program succeeded.
func TestRegisterResourceFailsWhenHookRegistrationIsRejected(t *testing.T) {
	t.Parallel()

	m := &hookMonitor{supportsHooks: true, hookRejection: status.Error(codes.InvalidArgument, "hook refused")}

	err := runWithHook(t, m)

	require.Error(t, err, "a rejected hook registration must not end in success")
	assert.Contains(t, err.Error(), "hook refused")
	assert.NotContains(t, m.names(), "thing", "the resource must not be registered without its hook")
}

// The same for an engine that does not support resource hooks at all.
func TestRegisterResourceFailsWhenEngineDoesNotSupportHooks(t *testing.T) {
	t.Parallel()

	m := &hookMonitor{supportsHooks: false}

	err := runWithHook(t, m)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support resource hooks")
	assert.NotContains(t, m.names(), "thing")
}

// Control: with support and an accepted registration the resource is registered with its hook.
func TestRegisterResourceWithAcceptedHook(t *testing.T) {
	t.Parallel()

	m := &hookMonitor{supportsHooks: true}

	require.NoError(t, runWithHook(t, m))

	require.Contains(t, m.names(), "thing")
	for _, r := range m.registered {
		if r.Name == "thing" {
			require.Equal(t, []string{"guard"}, r.GetHooks().GetBeforeCreate())
		}
	}
}
