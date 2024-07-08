// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package owner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestElectOwner(t *testing.T) {
	ts := newEtcdTestSuite(t, electionConfigForTest(1), "key")
	t.Cleanup(ts.close)

	// 2 nodes start and 1 node is the owner
	{
		elec1 := ts.newElection("1")
		elec2 := ts.newElection("2")
		elec1.Start(context.Background())
		elec2.Start(context.Background())
		ownerID := ts.getOwnerID()
		ts.expectEvent(ownerID, eventTypeElected)
	}
	// stop the owner and the other becomes the owner
	{
		ownerID := ts.getOwnerID()
		elec := ts.getElection(ownerID)
		elec.Close()
		ts.expectEvent(ownerID, eventTypeRetired)
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
		ownerID := ts.getOwnerID()
		elec = ts.getElection(ownerID)
		elec.Close()
		ts.expectEvent(ownerID, eventTypeRetired)
		_, err := elec.GetOwnerID(context.Background())
		require.Error(t, err)
	}
}

func TestEtcdServerDown(t *testing.T) {
	ts := newEtcdTestSuite(t, electionConfigForTest(1), "key")
	t.Cleanup(ts.close)

	elec1 := ts.newElection("1")
	elec1.Start(context.Background())
	ts.expectEvent("1", eventTypeElected)

	// server is down
	addr := ts.shutdownServer()
	_, err := elec1.GetOwnerID(context.Background())
	require.Error(t, err)
	ts.startServer(addr)
	// the previous owner only retires when the new one is elected
	ts.expectEvent("1", eventTypeRetired, eventTypeElected)
	ownerID := ts.getOwnerID()
	require.Equal(t, "1", ownerID)

	// server is down and start another member
	addr = ts.shutdownServer()
	_, err = elec1.GetOwnerID(context.Background())
	require.Error(t, err)
	elec2 := ts.newElection("2")
	elec2.Start(context.Background())

	// start the server again and the elections recover
	ts.startServer(addr)
	ownerID = ts.getOwnerID()
	ts.expectEvent("1", eventTypeRetired)
	ts.expectEvent(ownerID, eventTypeElected)
}

func TestOwnerHang(t *testing.T) {
	ts := newEtcdTestSuite(t, electionConfigForTest(1), "key")
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
