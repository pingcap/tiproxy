// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package elect

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestElectOwner(t *testing.T) {
	ts := newEtcdTestSuite(t, "key")
	t.Cleanup(ts.close)

	// 2 nodes start and 1 node is the owner
	{
		elec1 := ts.newElection("1")
		elec2 := ts.newElection("2")
		elec1.Start(context.Background())
		elec2.Start(context.Background())
		require.Equal(t, "1", elec1.ID())
		require.Equal(t, "2", elec2.ID())
		ownerID := ts.getOwnerID()
		ts.expectEvent(ownerID, eventTypeElected)
	}
	// stop the owner and the other becomes the owner
	{
		ownerID := ts.getOwnerID()
		elec := ts.getElection(ownerID)
		elec.Close()
		ts.expectNoEvent(ownerID)
		ownerID2 := ts.getOwnerID()
		require.NotEqual(t, ownerID, ownerID2)
		ts.expectEvent(ownerID2, eventTypeElected)
	}
	// start a new node and the owner doesn't change
	{
		ownerID := ts.getOwnerID()
		elec := ts.newElection("3")
		elec.Start(context.Background())
		time.Sleep(300 * time.Millisecond)
		ownerID2 := ts.getOwnerID()
		require.Equal(t, ownerID, ownerID2)
	}
	// stop all the nodes and there's no owner
	{
		elec := ts.getElection("3")
		elec.Close()
		ts.expectNoEvent("3")
		ownerID := ts.getOwnerID()
		elec = ts.getElection(ownerID)
		elec.Close()
		ts.expectNoEvent(ownerID)
		_, err := elec.GetOwnerID(context.Background())
		require.Error(t, err)
	}
	// double closing elections is allowed
	{
		elec := ts.getElection("3")
		elec.Close()
	}
}

func TestEtcdServerDown(t *testing.T) {
	ts := newEtcdTestSuite(t, "key")
	t.Cleanup(ts.close)

	elec1 := ts.newElection("1")
	elec1.Start(context.Background())
	ts.expectEvent("1", eventTypeElected)

	// server is down
	addr := ts.shutdownServer()
	_, err := elec1.GetOwnerID(context.Background())
	require.Error(t, err)
	// the owner should not retire before the server is up again
	ts.expectNoEvent("1")
	ts.startServer(addr)
	// the owner should not retire because there's no other member
	ts.expectNoEvent("1")
	ownerID := ts.getOwnerID()
	require.Equal(t, "1", ownerID)

	// server is down and start another member
	addr = ts.shutdownServer()
	_, err = elec1.GetOwnerID(context.Background())
	require.Error(t, err)
	elec2 := ts.newElection("2")
	elec2.Start(context.Background())
	// the owner should not retire before the server is up again
	ts.expectNoEvent("1")

	// start the server again and the elections recover
	ts.startServer(addr)
	ownerID = ts.getOwnerID()
	if ownerID == "1" {
		ts.expectNoEvent("1")
	} else {
		ts.expectEvent("1", eventTypeRetired)
		ts.expectEvent(ownerID, eventTypeElected)
	}
}

func TestOwnerHang(t *testing.T) {
	ts := newEtcdTestSuite(t, "key")
	t.Cleanup(ts.close)

	// make the owner hang at loop
	elec1 := ts.newElection("1")
	ts.hang("1", true)
	defer ts.hang("1", false)
	elec1.Start(context.Background())
	ownerID := ts.getOwnerID()
	require.Equal(t, "1", ownerID)

	// start another member
	elec2 := ts.newElection("2")
	elec2.Start(context.Background())
	// even if the owner hangs, it's keeping alive at background
	time.Sleep(time.Second)
	ownerID = ts.getOwnerID()
	require.Equal(t, "1", ownerID)
}

func TestOwnerMetric(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	checkMetric := func(key string, expectedFound bool) {
		results, err := metrics.Collect(metrics.OwnerGauge)
		require.NoError(t, err)
		found := false
		for _, result := range results {
			if *result.Label[0].Value == key {
				require.EqualValues(t, 1, *result.Gauge.Value)
				found = true
				break
			}
		}
		require.Equal(t, expectedFound, found)
	}

	elec1 := NewElection(lg, nil, electionConfigForTest(1), "1", ownerKeyPrefix+"key"+ownerKeySuffix, newMockMember())
	elec1.onElected()
	checkMetric("key", true)

	elec2 := NewElection(lg, nil, electionConfigForTest(1), "1", "key2/1", newMockMember())
	elec2.onElected()
	checkMetric("key2/1", true)

	elec3 := NewElection(lg, nil, electionConfigForTest(1), "1", ownerKeyPrefix+"key3/1", newMockMember())
	elec3.onElected()
	checkMetric("key3/1", true)

	elec1.onRetired()
	checkMetric("key", false)
	checkMetric("key2/1", true)
	checkMetric("key3/1", true)
}
