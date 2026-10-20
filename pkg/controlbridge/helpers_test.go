// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Shared test doubles. These outlived the legacy RouterAdapter deleted at
// #223 Phase 2: the residual route-owner handler, the snapshot publisher and
// the metering tests all still need a control peer.

package controlbridge

import (
	"context"
	"errors"
	"sync"
	"testing"

	controlpb "github.com/pingcap/tiproxy/pkg/controlbridge/pb"
	"github.com/pingcap/tiproxy/pkg/controlbridge/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fakeSender struct {
	mu           sync.Mutex
	epoch        uint64
	nextID       uint64
	capabilities map[uint64]struct{}
	messages     []*controlpb.ControlEnvelope
}

func newFakeSender(epoch uint64, capabilities ...uint64) *fakeSender {
	peer := &fakeSender{epoch: epoch, capabilities: make(map[uint64]struct{}, len(capabilities))}
	for _, capability := range capabilities {
		peer.capabilities[capability] = struct{}{}
	}
	return peer
}

func (peer *fakeSender) Send(_ context.Context, envelope *controlpb.ControlEnvelope) error {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	cloned, ok := proto.Clone(envelope).(*controlpb.ControlEnvelope)
	if !ok {
		return errors.New("clone fake envelope")
	}
	cloned.ControlEpoch = peer.epoch
	peer.messages = append(peer.messages, cloned)
	return nil
}

func (peer *fakeSender) Epoch() uint64 { return peer.epoch }

func (peer *fakeSender) HasCapability(capability uint64) bool {
	_, ok := peer.capabilities[capability]
	return ok
}

func (peer *fakeSender) AllocateRequestID() (uint64, error) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.nextID == ^uint64(0) {
		return 0, transport.ErrRequestIDExhausted
	}
	peer.nextID++
	return peer.nextID, nil
}

func lastEnvelope(t *testing.T, peer *fakeSender) *controlpb.ControlEnvelope {
	t.Helper()
	peer.mu.Lock()
	defer peer.mu.Unlock()
	require.NotEmpty(t, peer.messages)
	return peer.messages[len(peer.messages)-1]
}
