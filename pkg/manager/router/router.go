// Copyright 2022 PingCAP, Inc.
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

package router

import (
	"time"

	glist "github.com/bahlo/generic-list-go"
	"github.com/pingcap/TiProxy/lib/util/errors"
)

var (
	ErrNoInstanceToSelect = errors.New("no instances to route")
)

// ConnEventReceiver receives connection events.
type ConnEventReceiver interface {
	OnRedirectSucceed(from, to string, conn RedirectableConn) error
	OnRedirectFail(from, to string, conn RedirectableConn) error
	OnConnClosed(addr string, conn RedirectableConn) error
}

// Router routes client connections to backends.
type Router interface {
	// Router will handle connection events to balance connections if possible.
	ConnEventReceiver

	GetBackendSelector() BackendSelector
	RedirectConnections() error
	ConnCount() int
	Close()
}

type connPhase int

const (
	// The session is never redirected.
	phaseNotRedirected connPhase = iota
	// The session is redirecting.
	phaseRedirectNotify
	// The session redirected successfully last time.
	phaseRedirectEnd
	// The session failed to redirect last time.
	phaseRedirectFail
)

const (
	// The interval to rebalance connections.
	rebalanceInterval = 10 * time.Millisecond
	// The number of connections to rebalance during each interval.
	// Limit the number to avoid creating too many connections suddenly on a backend.
	rebalanceConnsPerLoop = 10
	// The threshold of ratio of the highest score and lowest score.
	// If the ratio exceeds the threshold, the proxy will rebalance connections.
	rebalanceMaxScoreRatio = 1.2
	// After a connection fails to redirect, it may contain some unmigratable status.
	// Limit its redirection interval to avoid unnecessary retrial to reduce latency jitter.
	redirectFailMinInterval = 3 * time.Second
)

// RedirectableConn indicates a redirect-able connection.
type RedirectableConn interface {
	SetEventReceiver(receiver ConnEventReceiver)
	SetValue(key, val any)
	Value(key any) any
	Redirect(addr string)
	GetRedirectingAddr() string
	NotifyBackendStatus(status BackendStatus)
}

// backendWrapper contains the connections on the backend.
type backendWrapper struct {
	status BackendStatus
	addr   string
	// A list of *connWrapper and is ordered by the connecting or redirecting time.
	// connList and connMap include moving out connections but not moving in connections.
	connList *glist.List[*connWrapper]
}

// score calculates the score of the backend. Larger score indicates higher load.
func (b *backendWrapper) score() int {
	return b.status.ToScore() + b.connList.Len()
}

// connWrapper wraps RedirectableConn.
type connWrapper struct {
	RedirectableConn
	phase connPhase
	// Last redirect start time of this connection.
	lastRedirect time.Time
}
