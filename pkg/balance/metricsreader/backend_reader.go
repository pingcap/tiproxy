// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metricsreader

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/http"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/siddontang/go/hack"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// readerOwnerKeyPrefix is the key prefix in etcd for backend reader owner election.
	// For global owner, the key is "/tiproxy/metric_reader/owner".
	// For zonal owner, the key is "/tiproxy/metric_reader/{zone}/owner".
	readerOwnerKeyPrefix = "/tiproxy/metric_reader"
	readerOwnerKeySuffix = "owner"
	// sessionTTL is the session's TTL in seconds for backend reader owner election.
	sessionTTL = 15
	// backendMetricPath is the path of backend HTTP API to read metrics.
	backendMetricPath = "/metrics"
	// ownerMetricPath is the path of reading backend metrics from the backend reader owner.
	ownerMetricPath = "/api/backend/metrics"
	goPoolSize      = 100
	goMaxIdle       = time.Minute
)

var (
	errReadMetrics = errors.New("read backend metrics failed")
)

type backendHistory struct {
	Step1History []model.SamplePair
	Step2History []model.SamplePair
}

type BackendReader struct {
	sync.Mutex
	// rule key: QueryRule
	queryRules map[string]QueryRule
	// rule key: QueryResult
	queryResults map[string]QueryResult
	// the owner generates the history from querying backends and other members query the history from the owner
	// rule key: {backend name: backendHistory}
	history map[string]map[string]backendHistory
	// the owner marshalles history to share it to other members
	// cache the marshalled history to avoid duplicated marshalling
	marshalledHistory []byte
	cfgGetter         config.ConfigGetter
	backendFetcher    TopologyFetcher
	lastZone          string
	electionCfg       elect.ElectionConfig
	election          elect.Election
	isOwner           atomic.Bool
	wgp               *waitgroup.WaitGroupPool
	etcdCli           *clientv3.Client
	httpCli           *http.Client
	lg                *zap.Logger
	cfg               *config.HealthCheck
}

func NewBackendReader(lg *zap.Logger, cfgGetter config.ConfigGetter, httpCli *http.Client, etcdCli *clientv3.Client,
	backendFetcher TopologyFetcher, cfg *config.HealthCheck) *BackendReader {
	return &BackendReader{
		queryRules:     make(map[string]QueryRule),
		queryResults:   make(map[string]QueryResult),
		history:        make(map[string]map[string]backendHistory),
		lg:             lg,
		cfgGetter:      cfgGetter,
		backendFetcher: backendFetcher,
		cfg:            cfg,
		wgp:            waitgroup.NewWaitGroupPool(goPoolSize, goMaxIdle),
		electionCfg:    elect.DefaultElectionConfig(sessionTTL),
		etcdCli:        etcdCli,
		httpCli:        httpCli,
	}
}

func (br *BackendReader) Start(ctx context.Context) error {
	cfg := br.cfgGetter.GetConfig()
	return br.initElection(ctx, cfg)
}

func (br *BackendReader) initElection(ctx context.Context, cfg *config.Config) error {
	ip, _, statusPort, err := cfg.GetIPPort()
	if err != nil {
		return err
	}

	// Use the status address as the key so that it can read metrics from the address.
	id := net.JoinHostPort(ip, statusPort)
	var key string
	br.lastZone = cfg.GetLocation()
	if len(br.lastZone) > 0 {
		key = fmt.Sprintf("%s/%s/%s", readerOwnerKeyPrefix, br.lastZone, readerOwnerKeySuffix)
	} else {
		key = fmt.Sprintf("%s/%s", readerOwnerKeyPrefix, readerOwnerKeySuffix)
	}
	br.election = elect.NewElection(br.lg.Named("elect"), br.etcdCli, br.electionCfg, id, key, br)
	br.election.Start(ctx)
	return nil
}

func (br *BackendReader) OnElected() {
	br.isOwner.Store(true)
}

func (br *BackendReader) OnRetired() {
	br.isOwner.Store(false)
}

func (br *BackendReader) AddQueryRule(key string, rule QueryRule) {
	br.Lock()
	defer br.Unlock()
	br.queryRules[key] = rule
}

func (br *BackendReader) RemoveQueryRule(key string) {
	br.Lock()
	defer br.Unlock()
	delete(br.queryRules, key)
}

func (br *BackendReader) GetQueryResult(key string) QueryResult {
	br.Lock()
	defer br.Unlock()
	// Return an empty QueryResult if it's not found.
	return br.queryResults[key]
}

func (br *BackendReader) ReadMetrics(ctx context.Context) error {
	// If the zone changes, start a new election.
	cfg := br.cfgGetter.GetConfig()
	zone := cfg.GetLocation()
	if zone != br.lastZone {
		br.election.Close()
		if err := br.initElection(ctx, cfg); err != nil {
			return err
		}
	}

	// Read from all owners, regardless of whether the owner is a zone owner or global owner.
	zones, owners, err := br.queryAllOwners(ctx)
	if err != nil {
		return err
	}

	// If self is a owner, read the backends that are not read by any other owners.
	// Reading from backends before reading from other owners to avoid growing the history infinitely,
	// see https://github.com/pingcap/tiproxy/issues/638.
	var errs []error
	var backendLabels []string
	if br.isOwner.Load() {
		if idx := slices.Index(zones, zone); idx >= 0 {
			zones = slices.Delete(zones, idx, idx+1)
		}
		backendLabels, err = br.readFromBackends(ctx, zones)
		if err != nil {
			errs = append(errs, err)
		}
	}

	for _, owner := range owners {
		if owner == br.election.ID() {
			continue
		}
		br.lg.Info("read metrics from owner", zap.String("owner", owner))
		if err = br.readFromOwner(ctx, owner); err != nil {
			errs = append(errs, err)
		}
	}

	// Purge expired history.
	br.purgeHistory()
	// Marshal backend history for other members to query.
	if err := br.marshalHistory(backendLabels); err != nil {
		br.lg.Error("marshal backend history failed", zap.Any("addrs", backendLabels), zap.Error(err))
	}
	// Generate query result for all backends.
	br.history2QueryResult()
	if len(errs) > 0 {
		return errors.Collect(errReadMetrics, errs...)
	}
	return nil
}

// Query all owners, including zone owner and global owner.
func (br *BackendReader) queryAllOwners(ctx context.Context) (zones, owners []string, err error) {
	// Get all owner keys.
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	var kvs []*mvccpb.KeyValue
	kvs, err = etcd.GetKVs(ctx, br.etcdCli, readerOwnerKeyPrefix, opts, br.electionCfg.Timeout, br.electionCfg.RetryIntvl, br.electionCfg.RetryCnt)
	if err != nil {
		return
	}

	type ownerInfo struct {
		addr     string
		revision int64
	}
	// Multiple members campaign for the same owner key, so there exist multiple keys prefixed with the same owner key.
	// Choose the one with the least create revision for the same zone.
	ownerMap := make(map[string]ownerInfo)
	for _, kv := range kvs {
		key := hack.String(kv.Key)
		key = key[len(readerOwnerKeyPrefix):]
		if len(key) == 0 || key[0] != '/' {
			continue
		}
		key = key[1:]

		var zone string
		if strings.HasPrefix(key, readerOwnerKeySuffix) {
			// global owner key, such as "/tiproxy/metric_reader/owner/leaseID"
		} else if endIdx := strings.Index(key, "/"); endIdx > 0 && strings.HasPrefix(key[endIdx+1:], readerOwnerKeySuffix) {
			// zonal owner key, such as "/tiproxy/metric_reader/east/owner/leaseID"
			zone = key[:endIdx]
		} else {
			continue
		}

		if info, ok := ownerMap[zone]; !ok || info.revision > kv.CreateRevision {
			ownerMap[zone] = ownerInfo{
				addr:     hack.String(kv.Value),
				revision: kv.CreateRevision,
			}
		}
	}

	owners = make([]string, 0, len(ownerMap))
	zones = make([]string, 0, len(ownerMap))
	for zone, info := range ownerMap {
		if len(zone) > 0 && !slices.Contains(zones, zone) {
			zones = append(zones, zone)
		}
		if !slices.Contains(owners, info.addr) {
			owners = append(owners, info.addr)
		}
	}
	return
}

// If self is a owner, read backends except excludeZones. The backends in those zones are read by other zonal owners.
//
// If the zone is not set, there is only one global owner, who queries all backends.
// If the zone is set, there are several zonal owners, who query the backends in the same zone.
// There are some exceptions:
// 1. In k8s, the zone is not set at startup and then is set by HTTP API, so there may temporarily exist both global and zonal owners.
// 2. Some backends may not be in the same zone with any owner. E.g. there are only 2 TiProxy in a 3-AZ cluster.
// In any way, the owner queries the backends that are not queried by other owners.
func (br *BackendReader) readFromBackends(ctx context.Context, excludeZones []string) ([]string, error) {
	addrs, err := br.getBackendAddrs(ctx, excludeZones)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, nil
	}
	allNames := br.collectAllNames()
	if len(allNames) == 0 {
		return nil, nil
	}

	backendLabels := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		backendLabels = append(backendLabels, getLabel4Addr(addr))
	}
	br.lg.Info("read metrics from backends", zap.Strings("addrs", backendLabels))
	for i := range addrs {
		func(addr, label string) {
			br.wgp.RunWithRecover(func() {
				if ctx.Err() != nil {
					return
				}
				resp, err := br.readBackendMetric(ctx, addr)
				if err != nil {
					br.lg.Debug("read metrics from backend failed", zap.String("addr", addr), zap.Error(err))
					return
				}
				text := filterMetrics(hack.String(resp), allNames)
				mf, err := parseMetrics(text)
				if err != nil {
					br.lg.Warn("parse metrics failed", zap.String("addr", addr), zap.Error(err))
					return
				}
				br.metric2History(mf, label)
			}, nil, br.lg)
		}(addrs[i], backendLabels[i])
	}
	br.wgp.Wait()
	return backendLabels, nil
}

func (br *BackendReader) collectAllNames() []string {
	br.Lock()
	defer br.Unlock()
	names := make([]string, 0, len(br.queryRules)*3)
	for _, rule := range br.queryRules {
		for _, name := range rule.Names {
			if slices.Index(names, name) < 0 {
				names = append(names, name)
			}
		}
	}
	return names
}

func (br *BackendReader) readBackendMetric(ctx context.Context, addr string) ([]byte, error) {
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(br.cfg.RetryInterval), uint64(br.cfg.MaxRetries)), ctx)
	return br.httpCli.Get(addr, backendMetricPath, b, br.cfg.DialTimeout)
}

// metric2History appends the metrics to history for each rule of one backend.
func (br *BackendReader) metric2History(mfs map[string]*dto.MetricFamily, backend string) {
	now := model.TimeFromUnixNano(time.Now().UnixNano())
	br.Lock()
	defer br.Unlock()

	for ruleKey, rule := range br.queryRules {
		// If the metric doesn't exist, skip it.
		metricExists := true
		for _, name := range rule.Names {
			if _, ok := mfs[name]; !ok {
				metricExists = false
				break
			}
		}
		if !metricExists {
			continue
		}

		// step 1: get the latest pair (at a timepoint) and add it to step1History
		// E.g. calculate process_cpu_seconds_total/tidb_server_maxprocs
		sampleValue := rule.Metric2Value(mfs)
		if math.IsNaN(float64(sampleValue)) {
			continue
		}
		pair := model.SamplePair{Timestamp: now, Value: sampleValue}
		ruleHistory, ok := br.history[ruleKey]
		if !ok {
			ruleHistory = make(map[string]backendHistory)
			br.history[ruleKey] = ruleHistory
		}
		beHistory := ruleHistory[backend]
		beHistory.Step1History = append(beHistory.Step1History, pair)

		// step 2: get the latest pair by the history and add it to step2History
		// E.g. calculate irate(process_cpu_seconds_total/tidb_server_maxprocs[30s])
		sampleValue = rule.Range2Value(beHistory.Step1History)
		if !math.IsNaN(float64(sampleValue)) {
			beHistory.Step2History = append(beHistory.Step2History, model.SamplePair{Timestamp: now, Value: sampleValue})
		}
		ruleHistory[backend] = beHistory
	}
}

// history2QueryResult generates new query results from the history.
func (br *BackendReader) history2QueryResult() {
	now := time.Now()
	br.Lock()
	defer br.Unlock()

	queryResults := make(map[string]QueryResult, len(br.queryRules))
	for ruleKey, rule := range br.queryRules {
		ruleHistory := br.history[ruleKey]
		if len(ruleHistory) == 0 {
			continue
		}

		var value model.Value
		switch rule.ResultType {
		case model.ValVector:
			results := make([]*model.Sample, 0, len(ruleHistory))
			for backend, beHistory := range ruleHistory {
				if len(beHistory.Step2History) == 0 {
					continue
				}
				labels := map[model.LabelName]model.LabelValue{LabelNameInstance: model.LabelValue(backend)}
				// vector indicates returning the latest pair
				lastPair := beHistory.Step2History[len(beHistory.Step2History)-1]
				results = append(results, &model.Sample{Value: lastPair.Value, Timestamp: lastPair.Timestamp, Metric: labels})
			}
			value = model.Vector(results)
		case model.ValMatrix:
			results := make([]*model.SampleStream, 0, len(ruleHistory))
			for backend, beHistory := range ruleHistory {
				if len(beHistory.Step2History) == 0 {
					continue
				}
				labels := map[model.LabelName]model.LabelValue{LabelNameInstance: model.LabelValue(backend)}
				// matrix indicates returning the history
				// copy a slice to avoid data race
				pairs := make([]model.SamplePair, len(beHistory.Step2History))
				copy(pairs, beHistory.Step2History)
				results = append(results, &model.SampleStream{Values: pairs, Metric: labels})
			}
			value = model.Matrix(results)
		default:
			br.lg.Error("unsupported value type", zap.String("value type", rule.ResultType.String()))
		}

		queryResults[ruleKey] = QueryResult{
			Value:      value,
			UpdateTime: now,
		}
	}

	br.queryResults = queryResults
}

// purgeHistory purges the expired or useless history values, otherwise the memory grows infinitely.
func (br *BackendReader) purgeHistory() {
	now := time.Now()
	br.Lock()
	defer br.Unlock()
	for id, ruleHistory := range br.history {
		rule, ok := br.queryRules[id]
		// the rule is removed
		if !ok {
			delete(br.history, id)
			continue
		}
		for backend, backendHistory := range ruleHistory {
			beforeLen := len(backendHistory.Step1History)
			backendHistory.Step1History = purgeHistory(backendHistory.Step1History, rule.Retention, now)
			backendHistory.Step2History = purgeHistory(backendHistory.Step2History, rule.Retention, now)
			afterLen := len(backendHistory.Step1History)
			if id == "cpu" && backend == "tc-tidb-1" && len(backendHistory.Step1History) > 0 {
				br.lg.Info("purged", zap.Int("before", beforeLen), zap.Int("after", afterLen), zap.String("backend", backend), zap.Int64("retention", rule.Retention.Milliseconds()), zap.Int64("now", now.UnixMilli()), zap.Int64("ts", backendHistory.Step1History[0].Timestamp.Time().UnixMilli()))
			}
			// the history is expired, maybe the backend is down
			if len(backendHistory.Step1History) == 0 && len(backendHistory.Step2History) == 0 {
				delete(ruleHistory, backend)
			} else {
				ruleHistory[backend] = backendHistory
			}
		}
	}
}

func (br *BackendReader) GetBackendMetrics() []byte {
	br.Lock()
	defer br.Unlock()
	return br.marshalledHistory
}

// readFromOwner queries metric history from the owner.
// If every member queries directly from backends, the backends may suffer from too much pressure.
func (br *BackendReader) readFromOwner(ctx context.Context, ownerAddr string) error {
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(br.cfg.RetryInterval), uint64(br.cfg.MaxRetries)), ctx)
	resp, err := br.httpCli.Get(ownerAddr, ownerMetricPath, b, br.cfg.DialTimeout)
	if err != nil {
		return err
	}
	if len(resp) == 0 {
		return nil
	}

	var newHistory map[string]map[string]backendHistory
	if err := json.Unmarshal(resp, &newHistory); err != nil {
		return err
	}

	// If this instance becomes the owner in the next round, it can reuse the history.
	br.mergeHistory(newHistory)
	return nil
}

// If the history of one backend already exists, choose the latest one.
func (br *BackendReader) mergeHistory(newHistory map[string]map[string]backendHistory) {
	br.Lock()
	defer br.Unlock()
	for ruleKey, newRuleHistory := range newHistory {
		ruleHistory, ok := br.history[ruleKey]
		if !ok {
			br.history[ruleKey] = newRuleHistory
			continue
		}
		for backend, newBackendHistory := range newRuleHistory {
			backendHistory, ok := ruleHistory[backend]
			if !ok {
				ruleHistory[backend] = newBackendHistory
				continue
			}
			if len(backendHistory.Step1History) == 0 || (len(newBackendHistory.Step1History) > 0 &&
				newBackendHistory.Step1History[len(newBackendHistory.Step1History)-1].Timestamp.After(backendHistory.Step1History[len(backendHistory.Step1History)-1].Timestamp)) {
				backendHistory.Step1History = newBackendHistory.Step1History
			}
			if len(backendHistory.Step2History) == 0 || (len(newBackendHistory.Step2History) > 0 &&
				newBackendHistory.Step2History[len(newBackendHistory.Step2History)-1].Timestamp.After(backendHistory.Step2History[len(backendHistory.Step2History)-1].Timestamp)) {
				backendHistory.Step2History = newBackendHistory.Step2History
			}
			ruleHistory[backend] = backendHistory
		}
	}
}

// marshalHistory marshals the backends that are read by this owner. The marshaled data will be returned to other members.
func (br *BackendReader) marshalHistory(backends []string) error {
	br.Lock()
	defer br.Unlock()

	if len(backends) == 0 {
		br.marshalledHistory = nil
		return nil
	}

	filteredHistory := make(map[string]map[string]backendHistory, len(br.queryRules))
	for ruleKey, ruleHistory := range br.history {
		filteredRuleHistory := make(map[string]backendHistory, len(backends))
		filteredHistory[ruleKey] = filteredRuleHistory
		for backend, backendHistory := range ruleHistory {
			if slices.Contains(backends, backend) {
				filteredRuleHistory[backend] = backendHistory
			}
		}
	}

	marshalled, err := json.Marshal(filteredHistory)
	if err != nil {
		return errors.WithStack(err)
	}
	br.marshalledHistory = marshalled
	return nil
}

func (br *BackendReader) getBackendAddrs(ctx context.Context, excludeZones []string) ([]string, error) {
	backends, err := br.backendFetcher.GetTiDBTopology(ctx)
	if err != nil {
		br.lg.Error("failed to get backend addresses, stop reading metrics", zap.Error(err))
		return nil, err
	}
	addrs := make([]string, 0, len(backends))
	for _, backend := range backends {
		if len(excludeZones) > 0 {
			if slices.Contains(excludeZones, backend.Labels[config.LocationLabelName]) {
				continue
			}
		}
		addrs = append(addrs, net.JoinHostPort(backend.IP, strconv.Itoa(int(backend.StatusPort))))
	}
	return addrs, nil
}

func (br *BackendReader) PreClose() {
	if br.election != nil {
		br.election.Close()
	}
}

func (br *BackendReader) Close() {
	if br.election != nil {
		br.election.Close()
	}
}

func purgeHistory(history []model.SamplePair, retention time.Duration, now time.Time) []model.SamplePair {
	idx := -1
	for i := range history {
		if time.UnixMilli(int64(history[i].Timestamp)).Add(retention).After(now) {
			idx = i
			break
		}
	}
	if idx > 0 {
		copy(history[:], history[idx:])
		return history[:len(history)-idx]
	} else if idx < 0 {
		history = history[:0]
	}
	return history
}

// filterMetrics filters the necessary metrics so that it's faster to parse.
func filterMetrics(all string, names []string) string {
	var buffer strings.Builder
	buffer.Grow(4096)
	for {
		idx := strings.Index(all, "\n")
		var line string
		if idx < 0 {
			line = all
		} else {
			line = all[:idx+1]
			all = all[idx+1:]
		}
		for i := range names {
			// strings.Contains() includes the metric type in the result but it's slower.
			// Note that the result is always in `Metric.Untyped` because the metric type is ignored.
			if strings.HasPrefix(line, names[i]) {
				buffer.WriteString(line)
				break
			}
		}
		if idx < 0 {
			break
		}
	}
	return buffer.String()
}

func parseMetrics(text string) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	return parser.TextToMetricFamilies(strings.NewReader(text))
}
