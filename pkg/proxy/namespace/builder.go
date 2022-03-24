package namespace

import (
	"hash/crc32"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/djshow832/weir/pkg/proxy/router"
	wast "github.com/djshow832/weir/pkg/util/ast"
	"github.com/djshow832/weir/pkg/util/datastructure"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
)

type NamespaceImpl struct {
	name string
	Br   driver.Breaker
	Frontend
	router      driver.Router
	rateLimiter *NamespaceRateLimiter
}

func BuildNamespace(cfg *config.Namespace) (Namespace, error) {
	fe, err := BuildFrontend(&cfg.Frontend)
	if err != nil {
		return nil, errors.WithMessage(err, "build frontend error")
	}
	rt, err := BuildRouter(&cfg.Backend)
	if err != nil {
		return nil, errors.WithMessage(err, "build router error")
	}
	wrapper := &NamespaceImpl{
		name:     cfg.Namespace,
		Frontend: fe,
		router:   rt,
	}
	brm, err := NewBreaker(&cfg.Breaker)
	if err != nil {
		return nil, err
	}
	br, err := brm.GetBreaker()
	if err != nil {
		return nil, err
	}
	wrapper.Br = br

	rateLimiter := NewNamespaceRateLimiter(cfg.RateLimiter.Scope, cfg.RateLimiter.QPS)
	wrapper.rateLimiter = rateLimiter

	return wrapper, nil
}

func (n *NamespaceImpl) Name() string {
	return n.name
}

func (n *NamespaceImpl) GetBreaker() (driver.Breaker, error) {
	return n.Br, nil
}

func (n *NamespaceImpl) GetRateLimiter() driver.RateLimiter {
	return n.rateLimiter
}

func (n *NamespaceImpl) GetRouter() driver.Router {
	return n.router
}

func (n *NamespaceImpl) Close() {
}

func BuildRouter(cfg *config.BackendNamespace) (driver.Router, error) {
	if len(cfg.Instances) == 0 {
		return nil, errors.New("no instances for the backend")
	}
	rt := router.NewRandomRouter()
	rt.SetAddresses(cfg.Instances)
	return rt, nil
}

func BuildFrontend(cfg *config.FrontendNamespace) (Frontend, error) {
	fns := &FrontendNamespace{
		allowedDBs: cfg.AllowedDBs,
		usernames:  cfg.Usernames,
	}
	fns.allowedDBSet = datastructure.StringSliceToSet(cfg.AllowedDBs)

	sqlBlacklist := make(map[uint32]SQLInfo)
	fns.sqlBlacklist = sqlBlacklist

	p := parser.New()
	for _, deniedSQL := range cfg.SQLBlackList {
		stmtNodes, _, err := p.Parse(deniedSQL.SQL, "", "")
		if err != nil {
			return nil, err
		}
		if len(stmtNodes) != 1 {
			return nil, nil
		}
		v, err := wast.ExtractAstVisit(stmtNodes[0])
		if err != nil {
			return nil, err
		}
		fns.sqlBlacklist[crc32.ChecksumIEEE([]byte(v.SqlFeature()))] = SQLInfo{SQL: deniedSQL.SQL}
	}

	sqlWhitelist := make(map[uint32]SQLInfo)
	fns.sqlWhitelist = sqlWhitelist
	for _, allowedSQL := range cfg.SQLWhiteList {
		stmtNodes, _, err := p.Parse(allowedSQL.SQL, "", "")
		if err != nil {
			return nil, err
		}
		if len(stmtNodes) != 1 {
			return nil, nil
		}
		v, err := wast.ExtractAstVisit(stmtNodes[0])
		if err != nil {
			return nil, err
		}
		fns.sqlWhitelist[crc32.ChecksumIEEE([]byte(v.SqlFeature()))] = SQLInfo{SQL: allowedSQL.SQL}
	}

	return fns, nil
}

func DefaultAsyncCloseNamespace(ns Namespace) error {
	return nil
}
