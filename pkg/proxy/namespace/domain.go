package namespace

import (
	"github.com/tidb-incubator/weir/pkg/proxy/driver"
)

type Namespace interface {
	Name() string
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
	Close()
	GetBreaker() (driver.Breaker, error)
	GetRateLimiter() driver.RateLimiter
	GetRouter() driver.Router
}

type Frontend interface {
	IsDatabaseAllowed(db string) bool
	ListDatabases() []string
	IsDeniedSQL(sqlFeature uint32) bool
	IsAllowedSQL(sqlFeature uint32) bool
}
