package driver

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	gomysql "github.com/siddontang/go-mysql/mysql"
	pnet "github.com/tidb-incubator/weir/pkg/proxy/net"
	wast "github.com/tidb-incubator/weir/pkg/util/ast"
	wauth "github.com/tidb-incubator/weir/pkg/util/auth"
	cb "github.com/tidb-incubator/weir/pkg/util/rate_limit_breaker/circuit_breaker"
)

// Server information.
const (
	ServerStatusInTrans            uint16 = 0x0001
	ServerStatusAutocommit         uint16 = 0x0002
	ServerMoreResultsExists        uint16 = 0x0008
	ServerStatusNoGoodIndexUsed    uint16 = 0x0010
	ServerStatusNoIndexUsed        uint16 = 0x0020
	ServerStatusCursorExists       uint16 = 0x0040
	ServerStatusLastRowSend        uint16 = 0x0080
	ServerStatusDBDropped          uint16 = 0x0100
	ServerStatusNoBackslashEscaped uint16 = 0x0200
	ServerStatusMetadataChanged    uint16 = 0x0400
	ServerStatusWasSlow            uint16 = 0x0800
	ServerPSOutParams              uint16 = 0x1000
)

type QueryCtxImpl struct {
	connId      uint64
	nsmgr       NamespaceManager
	ns          Namespace
	currentDB   string
	parser      *parser.Parser
	sessionVars *SessionVarsWrapper
	connMgr     BackendConnManager
}

func NewQueryCtxImpl(nsmgr NamespaceManager, backendConnMgr BackendConnManager, connId uint64) *QueryCtxImpl {
	return &QueryCtxImpl{
		connId:      connId,
		nsmgr:       nsmgr,
		parser:      parser.New(),
		connMgr:     backendConnMgr,
		sessionVars: NewSessionVarsWrapper(variable.NewSessionVars()),
	}
}

func (q *QueryCtxImpl) Status() uint16 {
	return q.sessionVars.Status()
}

func (q *QueryCtxImpl) LastInsertID() uint64 {
	return q.sessionVars.LastInsertID()
}

func (q *QueryCtxImpl) LastMessage() string {
	return q.sessionVars.GetMessage()
}

func (q *QueryCtxImpl) AffectedRows() uint64 {
	return q.sessionVars.AffectedRows()
}

// TODO(eastfisher): implement this function
func (*QueryCtxImpl) Value(key fmt.Stringer) interface{} {
	return nil
}

// TODO(eastfisher): implement this function
func (*QueryCtxImpl) SetValue(key fmt.Stringer, value interface{}) {
	return
}

func (q *QueryCtxImpl) CurrentDB() string {
	return q.currentDB
}

func (q *QueryCtxImpl) Execute(ctx context.Context, sql string) (*gomysql.Result, error) {
	charsetInfo, collation := q.sessionVars.GetCharsetInfo()
	stmt, err := q.parser.ParseOneStmt(sql, charsetInfo, collation)
	if err != nil {
		return nil, err
	}

	tableName := wast.ExtractFirstTableNameFromStmt(stmt)
	ctx = wast.CtxWithAstTableName(ctx, tableName)

	_, digest := parser.NormalizeDigest(sql)
	sqlDigest := crc32.ChecksumIEEE([]byte(digest))

	if q.isStmtDenied(ctx, sqlDigest) {
		q.recordDeniedQueryMetrics(ctx, stmt)
		return nil, mysql.NewErrf(mysql.ErrUnknown, "statement is denied")
	}

	if q.isStmtAllowed(ctx, sqlDigest) {
		return q.execute(ctx, sql)
	}

	if !q.isStmtNeedToCheckCircuitBreaking(stmt) {
		return q.execute(ctx, sql)
	}

	if rateLimitKey, ok := q.getRateLimiterKey(ctx, q.ns.GetRateLimiter()); ok && rateLimitKey != "" {
		if err := q.ns.GetRateLimiter().Limit(ctx, rateLimitKey); err != nil {
			return nil, err
		}
	}

	return q.executeWithBreakerInterceptor(ctx, stmt, sql, sqlDigest)
}

func (q *QueryCtxImpl) executeWithBreakerInterceptor(ctx context.Context, stmtNode ast.StmtNode, sql string, sqlDigest uint32) (*gomysql.Result, error) {
	breaker, err := q.ns.GetBreaker()
	if err != nil {
		return nil, err
	}

	brName, ok := q.getBreakerName(ctx, sql, breaker)
	if !ok {
		return q.execute(ctx, sql)
	}

	status, brNum := breaker.Status(brName)
	if status == cb.CircuitBreakerStatusOpen {
		return nil, cb.ErrCircuitBreak
	}

	if status == cb.CircuitBreakerStatusHalfOpen {
		if !breaker.CASHalfOpenProbeSent(brName, brNum, true) {
			return nil, cb.ErrCircuitBreak
		}
	}

	var triggerFlag int32 = -1
	connId := q.connId
	if err := breaker.AddTimeWheelTask(brName, connId, &triggerFlag); err != nil {
		return nil, err
	}
	// TODO: handle err
	defer breaker.RemoveTimeWheelTask(connId)

	ret, err := q.execute(ctx, sql)

	if triggerFlag == -1 {
		// TODO: handle err
		breaker.Hit(brName, -1, false)
	}

	return ret, err
}

func (q *QueryCtxImpl) execute(ctx context.Context, sql string) (*gomysql.Result, error) {
	return q.executeStmt(ctx, sql)
}

func (q *QueryCtxImpl) ExecuteCmd(ctx context.Context, request []byte, clientIO *pnet.PacketIO) error {
	return q.connMgr.ExecuteCmd(ctx, request, clientIO)
}

func (q *QueryCtxImpl) Close() error {
	if q.ns != nil {
		q.ns.DescConnCount()
	}
	if q.connMgr != nil {
		return q.connMgr.Close()
	}
	return nil
}

func (q *QueryCtxImpl) ConnectBackend(ctx context.Context, clientIO *pnet.PacketIO) error {
	ns, ok := q.nsmgr.Auth("", nil, nil)
	if !ok {
		return errors.New("failed to find a namespace")
	}
	q.ns = ns
	addr, err := ns.GetRouter().Route()
	if err != nil {
		return err
	}
	if err = q.connMgr.Connect(ctx, addr, clientIO); err != nil {
		return err
	}
	q.ns.IncrConnCount()
	return nil
}

func (q *QueryCtxImpl) Auth(user *auth.UserIdentity, authData []byte, salt []byte) error {
	// Looks up namespace.
	ns, ok := q.nsmgr.Auth(user.Username, authData, salt)
	if !ok {
		return errors.New("Unrecognized user")
	}
	q.ns = ns
	authInfo := &wauth.AuthInfo{
		Username:   user.Username,
		AuthString: authData,
	}
	addr, err := ns.GetRouter().Route()
	if err != nil {
		return err
	}
	q.connMgr.SetAuthInfo(authInfo)
	if err = q.connMgr.Connect(nil, addr, nil); err != nil {
		return err
	}
	q.ns.IncrConnCount()
	return nil
}

// TODO(eastfisher): does weir need to support show processlist?
func (*QueryCtxImpl) ShowProcess() *util.ProcessInfo {
	return nil
}

func (q *QueryCtxImpl) GetSessionVars() *variable.SessionVars {
	return q.sessionVars.sessionVars
}

func (q *QueryCtxImpl) SetCommandValue(command byte) {
	q.sessionVars.SetCommandValue(command)
}

func (q *QueryCtxImpl) isStmtNeedToCheckCircuitBreaking(stmt ast.StmtNode) bool {
	breaker, err := q.ns.GetBreaker()
	if err != nil {
		return false
	}
	if !breaker.IsUseBreaker() {
		return false
	}
	switch stmt.(type) {
	case *ast.SelectStmt:
		return true
	case *ast.InsertStmt:
		return true
	case *ast.UpdateStmt:
		return true
	case *ast.DeleteStmt:
		return true
	default:
		return false
	}
}
