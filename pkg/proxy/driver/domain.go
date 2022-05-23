package driver

import (
	"context"
	"crypto/tls"
	"net"

	pnet "github.com/djshow832/weir/pkg/proxy/net"
)

type NamespaceManager interface {
	Auth(username string, pwd, salt []byte) (Namespace, bool)
	Close() error
}

type Namespace interface {
	Name() string
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
	IncrConnCount()
	DescConnCount()
	GetBreaker() (Breaker, error)
	GetRateLimiter() RateLimiter
	GetRouter() Router
}

type Breaker interface {
	IsUseBreaker() bool
	GetBreakerScope() string
	Hit(name string, idx int, isFail bool) error
	Status(name string) (int32, int)
	AddTimeWheelTask(name string, connectionID uint64, flag *int32) error
	RemoveTimeWheelTask(connectionID uint64) error
	CASHalfOpenProbeSent(name string, idx int, halfOpenProbeSent bool) bool
	CloseBreaker()
}

type RateLimiter interface {
	Scope() string
	Limit(ctx context.Context, key string) error
}

type Router interface {
	Route(RedirectableConn) (string, error)
	RedirectConnections() error
	Close()
}

type RedirectableConn interface {
	SetEventReceiver(receiver ConnEventReceiver)
	Redirect(addr string)
	ConnectionID() uint64
}

type ConnEventReceiver interface {
	OnRedirectSucceed(from, to string, conn RedirectableConn)
	OnRedirectFail(from, to string, conn RedirectableConn)
	OnConnClosed(addr string, conn RedirectableConn)
}

type Stmt interface {
	ID() int
	ParamNum() int
	ColumnNum() int
}

type ClientConnection interface {
	ConnectionID() uint64
	Addr() string
	Run(context.Context)
	Close() error
}

type BackendConnManager interface {
	RedirectableConn
	Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) error
	ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error
	Close() error
}

// QueryCtx is the interface to execute command.
type QueryCtx interface {
	ConnectBackend(ctx context.Context, clientIO *pnet.PacketIO, serverTLSConfig, backendTLSConfig *tls.Config) error

	ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error

	// Close closes the QueryCtx.
	Close() error
}

type IDriver interface {
	CreateClientConnection(conn net.Conn, connectionID uint64, serverTLSConfig, clusterTLSConfig *tls.Config) ClientConnection
}
