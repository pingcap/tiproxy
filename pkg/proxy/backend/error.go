// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

// These errors may not be disconnection errors. They are used for marking whether the error comes from the client or the backend.
var (
	ErrClientConn  = errors.New("this is an error from the client connection")
	ErrBackendConn = errors.New("this is an error from the backend connection")
)

// These errors are used to track internal errors.
var (
	ErrClientCap        = errors.New("Verify client capability failed, please upgrade the client")
	ErrClientHandshake  = errors.New("Fails to handshake with the client")
	ErrClientAuthFail   = errors.New("Authentication fails")
	ErrProxyErr         = errors.New("Other serverless error")
	ErrProxyNoBackend   = errors.New("No available TiDB instances, please make sure TiDB is available")
	ErrProxyNoTLS       = errors.New("Require TLS enabled on TiProxy when require-backend-tls=true")
	ErrBackendCap       = errors.New("Verify TiDB capability failed, please upgrade TiDB")
	ErrBackendHandshake = errors.New("TiProxy fails to connect to TiDB, please make sure TiDB is available")
	ErrBackendNoTLS     = errors.New("Require TLS enabled on TiDB when require-backend-tls=true")
	ErrBackendPPV2      = errors.New("TiProxy fails to connect to TiDB, please make sure TiDB proxy-protocol is set correctly. If this error still exists, please contact PingCAP")
)

// ErrToClient returns the error that needs to be sent to the client.
func ErrToClient(err error) error {
	switch {
	case pnet.IsMySQLError(err):
		// If it's a MySQL error, it should be already sent to the client.
		return nil
	case errors.Is(err, ErrProxyNoBackend):
		return ErrProxyNoBackend
	case errors.Is(err, ErrProxyNoTLS):
		return ErrProxyNoTLS
	case errors.Is(err, ErrBackendCap):
		return ErrBackendCap
	case errors.Is(err, ErrBackendHandshake):
		return ErrBackendHandshake
	case errors.Is(err, ErrBackendNoTLS):
		return ErrBackendNoTLS
	case errors.Is(err, ErrBackendPPV2):
		return ErrBackendPPV2
	case errors.Is(err, ErrProxyErr):
		// The error is returned by HandshakeHandler/BackendFetcher and wrapped with ErrProxyErr.
		return errors.Unwrap(err)
	}
	// For other errors, we don't send them to the client.
	return nil
}

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
	// SrcClientNetwork includes: EOF; reset by peer; connection refused; io timeout
	SrcClientNetwork
	// SrcClientHandshake includes: client capability unsupported; TLS handshake fails
	SrcClientHandshake
	// SrcClientAuthFail includes: backend returns auth fail
	SrcClientAuthFail
	// SrcClientSQLErr includes: SQL error
	SrcClientSQLErr
	// SrcProxyQuit includes: proxy graceful shutdown
	SrcProxyQuit
	// SrcProxyMalformed includes: malformed packet; invalid sequence
	SrcProxyMalformed
	// SrcProxyNoBackend includes: no backends
	SrcProxyNoBackend
	// SrcProxyErr includes: HandshakeHandler returns error; proxy disables TLS; unexpected errors
	SrcProxyErr
	// SrcBackendNetwork includes: EOF; reset by peer; connection refused; io timeout
	SrcBackendNetwork
	// SrcBackendHandshake includes: dial failure; backend capability unsupported; backend disables TLS; TLS handshake fails; proxy protocol fails
	SrcBackendHandshake
)

// Error2Source returns the ErrorSource by the error.
func Error2Source(err error) ErrorSource {
	if err == nil {
		return SrcNone
	}
	// Disconnection errors may come from other errors such as ErrProxyNoBackend and ErrBackendHandshake.
	// ErrClientConn and ErrBackendConn may include non-connection errors.
	if pnet.IsDisconnectError(err) {
		if errors.Is(err, ErrClientConn) {
			return SrcClientNetwork
		} else if errors.Is(err, ErrBackendConn) {
			return SrcBackendNetwork
		}
	}
	switch {
	// ErrInvalidSequence and ErrMalformPacket may be wrapped with other errors such as ErrBackendHandshake.
	case errors.Is(err, pnet.ErrInvalidSequence), errors.Is(err, mysql.ErrMalformPacket):
		// We assume the clients and TiDB are right and treat it as TiProxy bugs.
		return SrcProxyMalformed
	case errors.Is(err, ErrClientHandshake), errors.Is(err, ErrClientCap):
		return SrcClientHandshake
	case errors.Is(err, ErrClientAuthFail):
		return SrcClientAuthFail
	case errors.Is(err, ErrBackendHandshake), errors.Is(err, ErrBackendCap), errors.Is(err, ErrBackendNoTLS), errors.Is(err, ErrBackendPPV2):
		return SrcBackendHandshake
	case errors.Is(err, ErrProxyNoBackend):
		return SrcProxyNoBackend
	case pnet.IsMySQLError(err):
		// ErrClientAuthFail and ErrBackendHandshake may also contain MySQL error.
		return SrcClientSQLErr
	default:
		// All other untracked errors are proxy errors.
		return SrcProxyErr
	}
}

// String is used for metrics labels and log.
func (es ErrorSource) String() string {
	switch es {
	case SrcNone:
		return "success"
	case SrcClientNetwork:
		return "client network break"
	case SrcClientHandshake:
		return "client handshake fail"
	case SrcClientAuthFail:
		return "auth fail"
	case SrcClientSQLErr:
		return "SQL error"
	case SrcProxyQuit:
		return "proxy shutdown"
	case SrcProxyMalformed:
		return "malformed packet"
	case SrcProxyNoBackend:
		return "get backend fail"
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
	case SrcClientNetwork, SrcClientHandshake, SrcClientAuthFail, SrcClientSQLErr:
		return CompClient
	case SrcProxyQuit, SrcProxyMalformed, SrcProxyNoBackend, SrcProxyErr:
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
	case SrcNone, SrcProxyQuit, SrcClientNetwork, SrcClientSQLErr:
		return true
	}
	return false
}
