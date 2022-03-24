package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"

	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/siddontang/go-mysql/mysql"
	pnet "github.com/tidb-incubator/weir/pkg/proxy/net"
	wauth "github.com/tidb-incubator/weir/pkg/util/auth"
)

type NamespaceManager interface {
	Auth(username string, pwd, salt []byte) (Namespace, bool)
}

type Namespace interface {
	Name() string
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
	GetPooledConn(context.Context) (PooledBackendConn, error)
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
	SetAddresses([]string)
	Route() (string, error)
	AddConnOnAddr(string, int)
}

type PooledBackendConn interface {
	// PutBack put conn back to pool
	PutBack()

	// ErrorClose close conn and connpool create a new conn
	// call this function when conn is broken.
	ErrorClose() error
	BackendConn
}

type SimpleBackendConn interface {
	Close() error
	BackendConn
}

type BackendConn interface {
	Ping() error
	UseDB(dbName string) error
	GetDB() string
	Execute(command string, args ...interface{}) (*mysql.Result, error)
	Begin() error
	Commit() error
	Rollback() error
	StmtPrepare(sql string) (Stmt, error)
	StmtExecuteForward(data []byte) (*mysql.Result, error)
	StmtClosePrepare(stmtId int) error
	SetCharset(charset string) error
	FieldList(table string, wildcard string) ([]*mysql.Field, error)
	SetAutoCommit(bool) error
	IsAutoCommit() bool
	IsInTransaction() bool
	GetCharset() string
	GetConnectionID() uint32
	GetStatus() uint16
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
	SetAuthInfo(authInfo *wauth.AuthInfo)
	Connect(ctx context.Context, serverAddr string, clientIO *pnet.PacketIO) error
	ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error
	Query(ctx context.Context, sql string) (*mysql.Result, error)
	Close() error
}

// QueryCtx is the interface to execute command.
type QueryCtx interface {
	// Status returns server status code.
	Status() uint16

	// AffectedRows returns affected rows of last executed command.
	AffectedRows() uint64

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Execute executes a SQL statement.
	Execute(ctx context.Context, sql string) (*mysql.Result, error)

	ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error

	// Close closes the QueryCtx.
	Close() error

	ConnectBackend(ctx context.Context, clientIO *pnet.PacketIO) error

	// Auth verifies user's authentication.
	Auth(user *auth.UserIdentity, auth []byte, salt []byte) error

	// ShowProcess shows the information about the session.
	ShowProcess() *util.ProcessInfo

	// GetSessionVars return SessionVars.
	GetSessionVars() *variable.SessionVars

	SetCommandValue(command byte)
}

type IDriver interface {
	CreateClientConnection(conn net.Conn, connectionID uint64, tlsConfig *tls.Config, serverCapability uint32) ClientConnection
	CreateBackendConnManager() BackendConnManager
}
