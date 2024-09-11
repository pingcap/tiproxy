// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/conn"
	"go.uber.org/zap"
)

const (
	flushInterval = 10 * time.Second
)

type expCollection struct {
	count  uint64
	sample conn.Exception
}

func newExpCollection(e conn.Exception) *expCollection {
	return &expCollection{
		count:  1,
		sample: e,
	}
}

func (c *expCollection) inc() {
	c.count++
}

type ReportConfig struct {
	TlsConfig     *tls.Config
	flushInterval time.Duration
}

func (cfg *ReportConfig) Validate() error {
	if cfg.flushInterval == 0 {
		cfg.flushInterval = flushInterval
	}
	return nil
}

type Report interface {
	Start(ctx context.Context, cfg ReportConfig) error
	Close()
}

var _ Report = (*report)(nil)

type report struct {
	cfg         ReportConfig
	exceptions  map[conn.ExceptionType]map[string]*expCollection
	exceptionCh chan conn.Exception
	wg          waitgroup.WaitGroup
	cancel      context.CancelFunc
	db          ReportDB
	lg          *zap.Logger
}

func NewReport(lg *zap.Logger, exceptionCh chan conn.Exception, connCreator BackendConnCreator) *report {
	exceptions := make(map[conn.ExceptionType]map[string]*expCollection, conn.Total)
	for i := 0; i < int(conn.Total); i++ {
		exceptions[conn.ExceptionType(i)] = make(map[string]*expCollection)
	}
	return &report{
		db:          NewReportDB(lg.Named("db"), connCreator),
		lg:          lg,
		exceptions:  exceptions,
		exceptionCh: exceptionCh,
	}
}

func (r *report) Start(ctx context.Context, cfg ReportConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	r.cfg = cfg

	if err := r.db.Init(ctx); err != nil {
		return err
	}

	childCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.wg.RunWithRecover(func() {
		r.readExceptions(childCtx)
	}, nil, r.lg)
	return nil
}

func (r *report) readExceptions(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.flushInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case e := <-r.exceptionCh:
			m := r.exceptions[e.Type()]
			coll, ok := m[e.Key()]
			if !ok {
				m[e.Key()] = newExpCollection(e)
			} else {
				coll.inc()
			}
		case <-ticker.C:
			r.flush(ctx)
		}
	}
}

func (r *report) flush(ctx context.Context) {
	for tp, m := range r.exceptions {
		if err := r.db.InsertExceptions(ctx, tp, m); err != nil {
			r.lg.Error("insert exceptions failed", zap.Stringer("type", tp), zap.Error(err))
		}
		for k := range m {
			delete(m, k)
		}
	}
}

func (r *report) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	r.db.Close()
}
