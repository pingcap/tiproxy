// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/logger"
	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/monotime"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestFallback(t *testing.T) {
	// setup backend
	_, infos := setupTypicalBackendListener(t, "cpu 80.0\n")

	// setup prom
	promHttpHandler := newMockHttpHandler(t)
	port := promHttpHandler.Start()
	t.Cleanup(promHttpHandler.Close)
	promFetcher := newMockPromFetcher(port)
	f := func(reqBody string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"cpu"},"value":[1000,"100.0"]}]}}`
	}
	promHttpHandler.getRespBody.Store(&f)

	// setup rule
	expr := QueryExpr{
		PromQL: "cpu",
	}
	rule := QueryRule{
		Names:     []string{"cpu"},
		Retention: time.Minute,
		Metric2Value: func(mfs map[string]*dto.MetricFamily) model.SampleValue {
			return model.SampleValue(*mfs["cpu"].Metric[0].Untyped.Value)
		},
		Range2Value: func(pairs []model.SamplePair) model.SampleValue {
			return pairs[len(pairs)-1].Value
		},
		ResultType: model.ValVector,
	}

	// setup metrics reader
	lg, _ := logger.CreateLoggerForTest(t)
	cfg := newHealthCheckConfigForTest()
	cfgGetter := newMockConfigGetter(&config.Config{})
	backendFetcher := newMockBackendFetcher(infos, nil)
	httpCli := httputil.NewHTTPClient(func() *tls.Config { return nil })
	mr := NewDefaultMetricsReader(lg, promFetcher, backendFetcher, httpCli, cfg, cfgGetter)
	mr.AddQueryExpr("rule_id1", expr, rule)
	t.Cleanup(mr.Close)

	// setup backend reader
	election := newMockElection()
	mr.backendReader.election = election
	election.isOwner.Store(true)
	mr.backendReader.OnElected()

	// read from prom
	ts := monotime.Now()
	mr.readMetrics(context.Background())
	qr := mr.GetQueryResult("rule_id1")
	require.False(t, qr.Empty())
	require.Equal(t, model.SampleValue(100.0), qr.Value.(model.Vector)[0].Value)
	require.Equal(t, model.Time(1000000), qr.Value.(model.Vector)[0].Timestamp)
	require.GreaterOrEqual(t, qr.UpdateTime, ts)
	ts = qr.UpdateTime

	// read from backend
	promHttpHandler.statusCode.Store(http.StatusInternalServerError)
	mr.readMetrics(context.Background())
	qr = mr.GetQueryResult("rule_id1")
	require.False(t, qr.Empty())
	require.Equal(t, model.SampleValue(80.0), qr.Value.(model.Vector)[0].Value)
	require.GreaterOrEqual(t, qr.UpdateTime, ts)
}
