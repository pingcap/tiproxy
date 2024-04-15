// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

// Copyright 2020 Ipalfish, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"github.com/pingcap/tiproxy/pkg/balance/observer"
	"github.com/pingcap/tiproxy/pkg/balance/router"
)

type Namespace struct {
	name   string
	user   string
	bo     observer.BackendObserver
	router router.Router
}

func (n *Namespace) Name() string {
	return n.name
}

func (n *Namespace) User() string {
	return n.user
}

func (n *Namespace) GetRouter() router.Router {
	return n.router
}

func (n *Namespace) Close() {
	n.router.Close()
	n.bo.Close()
}
