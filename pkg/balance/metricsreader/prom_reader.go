// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"fmt"
	"maps"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"go.uber.org/zap"
)

type PromReader struct {
	sync.Mutex
	queryExprs   map[string]QueryExpr
	queryResults map[string]QueryResult
	promFetcher  PromInfoFetcher
	lg           *zap.Logger
	cfg          *config.HealthCheck
}

func NewPromReader(lg *zap.Logger, promFetcher PromInfoFetcher, cfg *config.HealthCheck) *PromReader {
	return &PromReader{
		lg:           lg,
		promFetcher:  promFetcher,
		cfg:          cfg,
		queryExprs:   make(map[string]QueryExpr),
		queryResults: make(map[string]QueryResult),
	}
}

func (pr *PromReader) ReadMetrics(ctx context.Context) error {
	// No PD, using static backends.
	if pr.promFetcher == nil || reflect.ValueOf(pr.promFetcher).IsNil() {
		return infosync.ErrNoProm
	}
	promQLAPI, err := pr.getPromAPI(ctx)
	if err != nil {
		return err
	}

	pr.Lock()
	copyedMap := make(map[string]QueryExpr, len(pr.queryExprs))
	maps.Copy(copyedMap, pr.queryExprs)
	pr.Unlock()
	results := make(map[string]QueryResult, len(copyedMap))
	now := time.Now()
	for id, expr := range copyedMap {
		qr, err := pr.queryMetric(ctx, promQLAPI, expr, now)
		// Stop and fallback to reading backends if prometheus is unavailable.
		if err != nil {
			return err
		}
		qr.UpdateTime = time.Now()
		results[id] = qr
	}
	pr.Lock()
	pr.queryResults = results
	pr.Unlock()
	return nil
}

// Always refresh the prometheus address just in case it changes.
func (pr *PromReader) getPromAPI(ctx context.Context) (promv1.API, error) {
	promInfo, err := pr.promFetcher.GetPromInfo(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: support TLS and authentication.
	promAddr := fmt.Sprintf("http://%s", net.JoinHostPort(promInfo.IP, strconv.Itoa(promInfo.Port)))
	promClient, err := api.NewClient(api.Config{
		Address: promAddr,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return promv1.NewAPI(promClient), nil
}

func (pr *PromReader) queryMetric(ctx context.Context, promQLAPI promv1.API, expr QueryExpr, curTime time.Time) (QueryResult, error) {
	promRange := expr.PromRange(curTime)
	if !expr.HasLabel {
		return pr.queryOnce(ctx, promQLAPI, expr.PromQL, promRange)
	}

	// The label key is `job` in TiUP but is `component` in TiOperator. We don't know which, so try them both.
	var qr QueryResult
	var err error
	for _, label := range [2]string{"job", "component"} {
		promQL := fmt.Sprintf(expr.PromQL, label)
		qr, err = pr.queryOnce(ctx, promQLAPI, promQL, promRange)
		if err == nil && !qr.Empty() {
			expr.PromQL = promQL
			expr.HasLabel = false
			break
		}
	}
	return qr, err
}

func (pr *PromReader) queryOnce(ctx context.Context, promQLAPI promv1.API, promQL string, promRange promv1.Range) (QueryResult, error) {
	childCtx, cancel := context.WithTimeout(ctx, pr.cfg.MetricsTimeout)
	var qr QueryResult
	err := backoff.Retry(func() error {
		var err error
		if promRange.Start.IsZero() {
			qr.Value, _, err = promQLAPI.Query(childCtx, promQL, time.Time{})
		} else {
			qr.Value, _, err = promQLAPI.QueryRange(childCtx, promQL, promRange)
		}
		if !pnet.IsRetryableError(err) {
			return backoff.Permanent(errors.WithStack(err))
		}
		return errors.WithStack(err)
	}, backoff.WithContext(backoff.NewExponentialBackOff(), childCtx))
	cancel()
	return qr, err
}

func (pr *PromReader) AddQueryExpr(key string, queryExpr QueryExpr) {
	pr.Lock()
	defer pr.Unlock()
	pr.queryExprs[key] = queryExpr
}

func (pr *PromReader) RemoveQueryExpr(key string) {
	pr.Lock()
	defer pr.Unlock()
	delete(pr.queryExprs, key)
}

func (pr *PromReader) GetQueryResult(key string) QueryResult {
	pr.Lock()
	defer pr.Unlock()
	// Return an empty QueryResult if it's not found.
	return pr.queryResults[key]
}

func (pr *PromReader) Close() {
}
