// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package router

type Factor interface {
	// Name returns the name of the factor.
	Name() string
	// Route returns the backends that a new connection should be routed to.
	// If multiple backends are healthy and balanced, return them all.
	Route(backendConns []*backendWrapper) []*backendWrapper
	// Balance returns the source and target backends that the router should migrate connections from and to.
	// If all backends are unhealthy, fromBackends contains all the backends and toBackends is empty.
	// If all backends are balanced, fromBackends is empty and toBackends contains all the backends.
	Balance(backendConns []*backendWrapper) (fromBackends, toBackends []*backendWrapper)
}
