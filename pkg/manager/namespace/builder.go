// Copyright 2020 Ipalfish, Inc.
// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package namespace

import (
	"hash/crc32"

	"github.com/pingcap/TiProxy/pkg/config"
	"github.com/pingcap/TiProxy/pkg/manager/router"
	"github.com/pingcap/TiProxy/pkg/proxy/driver"
	wast "github.com/pingcap/TiProxy/pkg/util/ast"
	"github.com/pingcap/TiProxy/pkg/util/datastructure"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type NamespaceImpl struct {
	name string
	Br   driver.Breaker
	Frontend
	router      driver.Router
	rateLimiter *NamespaceRateLimiter
}

func BuildNamespace(cfg *config.Namespace, client *clientv3.Client) (Namespace, error) {
	fe, err := BuildFrontend(&cfg.Frontend)
	if err != nil {
		return nil, errors.WithMessage(err, "build frontend error")
	}
	rt, err := BuildRouter(&cfg.Backend, client)
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
	n.router.Close()
}

func BuildRouter(cfg *config.BackendNamespace, client *clientv3.Client) (driver.Router, error) {
	return router.NewRandomRouter(cfg, client)
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
