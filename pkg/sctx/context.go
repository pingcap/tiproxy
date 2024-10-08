// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package sctx

import (
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/pkg/proxy/backend"
)

type Context struct {
	AdvertiseAddr string
	ConfigFile    string
	Handler       ServerHandler
}

type ServerHandler interface {
	backend.HandshakeHandler
	RegisterHTTP(c *gin.Engine) error
}
