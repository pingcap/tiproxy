// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

const (
	connectErrMsg         = "No available TiDB instances, please make sure TiDB is available"
	parsePktErrMsg        = "TiProxy fails to parse the packet, please contact PingCAP"
	handshakeErrMsg       = "TiProxy fails to connect to TiDB, please make sure TiDB is available"
	capabilityErrMsg      = "Verify TiDB capability failed, please upgrade TiDB"
	requireProxyTLSErrMsg = "Require TLS enabled on TiProxy when require-backend-tls=true"
	requireTiDBTLSErrMsg  = "Require TLS enabled on TiDB when require-backend-tls=true"
	checkPPV2ErrMsg       = "TiProxy fails to connect to TiDB, please make sure TiDB proxy-protocol is set correctly. If this error still exists, please contact PingCAP"
)

var (
	ErrClientConn       = errors.New("read or write client connection fail")
	ErrClientHandshake  = errors.New("handshake with client fail")
	ErrBackendConn      = errors.New("read or write backend connection fail")
	ErrBackendHandshake = errors.New("handshake with backend fail")
)

type SourceComp int

const (
	CompNone SourceComp = iota
	CompClient
	CompProxy
	CompBackend
)

type ErrorSource int

const (
	// SrcNone includes: succeed for OnHandshake; client normally quit for OnConnClose
	SrcNone ErrorSource = iota
	// SrcClientNetwork includes: EOF; reset by peer; connection refused; TLS handshake fails
	SrcClientNetwork
	// SrcClientHandshake includes: client capability unsupported
	SrcClientHandshake
	// SrcClientSQLErr includes: backend returns auth fail; SQL error
	SrcClientSQLErr
	// SrcProxyQuit includes: proxy graceful shutdown
	SrcProxyQuit
	// SrcProxyMalformed includes: malformed packet; invalid sequence
	SrcProxyMalformed
	// SrcProxyGetBackend includes: no backends
	SrcProxyGetBackend
	// SrcProxyErr includes: HandshakeHandler returns error; proxy disables TLS
	SrcProxyErr
	// SrcBackendNetwork includes: EOF; reset by peer; connection refused; TLS handshake fails
	SrcBackendNetwork
	// SrcBackendHandshake includes: backend capability unsupported; backend disables TLS
	SrcBackendHandshake
)

// Error2Source returns the ErrorSource by the error.
func Error2Source(err error) ErrorSource {
	switch {
	case err == nil:
		return SrcNone
	case errors.Is(err, pnet.ErrInvalidSequence) || errors.Is(err, gomysql.ErrMalformPacket):
		// We assume the clients and TiDB are right and treat it as TiProxy bugs.
		// ErrInvalidSequence may be wrapped with ErrClientConn or ErrBackendConn, so put it before other conditions.
		return SrcProxyErr
	case errors.Is(err, ErrClientConn):
		return SrcClientNetwork
	case errors.Is(err, ErrBackendConn):
		return SrcBackendNetwork
	case errors.Is(err, ErrClientHandshake):
		return SrcClientHandshake
	case errors.Is(err, ErrBackendHandshake):
		return SrcBackendHandshake
	case IsMySQLError(err):
		return SrcClientSQLErr
	default:
		return SrcProxyErr
	}
}

func (es ErrorSource) String() string {
	switch es {
	case SrcNone:
		return "ok"
	case SrcClientNetwork:
		return "client network break"
	case SrcClientHandshake:
		return "client handshake fail"
	case SrcClientSQLErr:
		return "client SQL error"
	case SrcProxyQuit:
		return "proxy shutdown"
	case SrcProxyMalformed:
		return "malformed packet"
	case SrcProxyGetBackend:
		return "proxy get backend fail"
	case SrcProxyErr:
		return "proxy error"
	case SrcBackendNetwork:
		return "backend network break"
	case SrcBackendHandshake:
		return "backend handshake fail"
	}
	return "unknown"
}

// GetSourceComp returns which component does this error belong to.
func (es ErrorSource) GetSourceComp() SourceComp {
	switch es {
	case SrcClientNetwork, SrcClientHandshake, SrcClientSQLErr:
		return CompClient
	case SrcProxyQuit, SrcProxyMalformed, SrcProxyGetBackend, SrcProxyErr:
		return CompProxy
	case SrcBackendNetwork, SrcBackendHandshake:
		return CompBackend
	default:
		return CompNone
	}
}

// Normal returns whether this error source is expected.
func (es ErrorSource) Normal() bool {
	switch es {
	case SrcNone, SrcProxyQuit:
		return true
	}
	return false
}
