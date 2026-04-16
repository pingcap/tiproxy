// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package elect

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"github.com/siddontang/go/hack"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	ownerKeyPrefix = "/tiproxy/"
	ownerKeySuffix = "/owner"
)

type Member interface {
	OnElected()
	OnRetired()
}

// Election is used to campaign the owner and manage the owner information.
type Election interface {
	// Start starts compaining the owner.
	Start(context.Context)
	// ID returns the member ID.
	ID() string
	// GetOwnerID gets the owner ID.
	GetOwnerID(ctx context.Context) (string, error)
	// Close stops campaigning and retires if this member currently owns the key.
	Close()
}

type ElectionConfig struct {
	Timeout    time.Duration
	RetryIntvl time.Duration
	RetryCnt   uint64
	SessionTTL int
}

func DefaultElectionConfig(sessionTTL int) ElectionConfig {
	return ElectionConfig{
		Timeout:    2 * time.Second,
		RetryIntvl: 500 * time.Millisecond,
		RetryCnt:   3,
		SessionTTL: sessionTTL,
	}
}

var _ Election = (*election)(nil)

// election is used for electing owner.
type election struct {
	cfg ElectionConfig
	// id is typically the instance address
	id  string
	key string
	// trimedKey is shown as a label in grafana
	trimedKey string
	lg        *zap.Logger
	etcdCli   *clientv3.Client
	wg        waitgroup.WaitGroup
	cancel    context.CancelFunc
	member    Member
	isOwner   bool
}

// NewElection creates an Election.
func NewElection(lg *zap.Logger, etcdCli *clientv3.Client, cfg ElectionConfig, id, key string, member Member) *election {
	lg = lg.With(zap.String("key", key), zap.String("id", id))
	return &election{
		lg:        lg,
		etcdCli:   etcdCli,
		cfg:       cfg,
		id:        id,
		key:       key,
		trimedKey: strings.TrimSuffix(strings.TrimPrefix(key, ownerKeyPrefix), ownerKeySuffix),
		member:    member,
	}
}

func (m *election) Start(ctx context.Context) {
	// No PD.
	if m.etcdCli == nil {
		return
	}
	clientCtx, cancelFunc := context.WithCancel(ctx)
	m.cancel = cancelFunc
	// Don't recover because we don't know what will happen after recovery.
	m.wg.Run(func() {
		m.campaignLoop(clientCtx)
	}, m.lg)
}

func (m *election) ID() string {
	return m.id
}

func (m *election) campaignLoop(ctx context.Context) {
	session, err := concurrency.NewSession(m.etcdCli, concurrency.WithTTL(m.cfg.SessionTTL), concurrency.WithContext(ctx))
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			m.lg.Error("new session failed, break campaign loop", zap.Error(errors.WithStack(err)))
		}
		return
	}
	for {
		m.lg.Debug("begin campaign")
		select {
		case <-session.Done():
			// Keep the local owner state until we observe a stronger signal that
			// another member has taken over. Retiring here would trade split-brain
			// risk for a no-owner window during etcd faults.
			m.lg.Info("etcd session is done, creates a new one")
			leaseID := session.Lease()
			session, err = concurrency.NewSession(m.etcdCli, concurrency.WithTTL(m.cfg.SessionTTL), concurrency.WithContext(ctx))
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					m.lg.Error("new session failed, break campaign loop", zap.Error(errors.WithStack(err)))
				}
				m.revokeLease(leaseID)
				return
			}
		case <-ctx.Done():
			m.revokeLease(session.Lease())
			return
		default:
		}
		// If the etcd server turns clocks forward, the following case may occur.
		// The etcd server deletes this session's lease ID, but etcd session doesn't find it.
		// In this case if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			if session != nil {
				err = session.Close()
				m.lg.Warn("etcd session encounters ErrLeaseNotFound, close it", zap.Error(err))
			}
			continue
		}

		elec := concurrency.NewElection(session, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			// Campaign may fail because etcd is temporarily unavailable while the
			// current owner lease is still valid. Retiring immediately here would
			// turn a transient etcd error into a local no-owner window.
			m.lg.Info("failed to campaign", zap.Error(errors.WithStack(err)))
			continue
		}

		kv, err := m.getOwnerInfo(ctx)
		if err != nil {
			// Failing to read the owner key does not necessarily mean this member
			// has lost its lease. Keep the current owner state until we see a
			// stronger signal such as session.Done, ErrLeaseNotFound, or an owner
			// mismatch.
			m.lg.Warn("failed to get owner info", zap.Error(err))
			continue
		}
		if hack.String(kv.Value) != m.id {
			// Campaign may finish without errors when the session is done.
			m.lg.Info("owner id mismatches", zap.String("owner", hack.String(kv.Value)))
			if m.isOwner {
				m.onRetired()
			}
			continue
		}

		if !m.isOwner {
			m.onElected()
		} else {
			// It was the owner before the etcd failure and now is still the owner.
			m.lg.Info("still the owner")
		}
		m.watchOwner(ctx, session, hack.String(kv.Key))
	}
}

func (m *election) onElected() {
	m.lg.Info("elected as the owner")
	m.member.OnElected()
	m.isOwner = true
	metrics.OwnerGauge.WithLabelValues(m.trimedKey).Set(1)
}

func (m *election) onRetired() {
	m.lg.Info("the owner retires")
	m.member.OnRetired()
	m.isOwner = false
	// Delete the metric so that it doesn't show on Grafana.
	metrics.OwnerGauge.MetricVec.DeletePartialMatch(map[string]string{metrics.LblType: m.trimedKey})
}

// revokeLease revokes the session lease so that other members can compaign immediately.
func (m *election) revokeLease(leaseID clientv3.LeaseID) {
	// If revoke takes longer than the ttl, lease is expired anyway.
	// Don't use the context of the caller because it may be already done.
	cancelCtx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.SessionTTL)*time.Second)
	if _, err := m.etcdCli.Revoke(cancelCtx, leaseID); err != nil {
		m.lg.Warn("revoke session failed", zap.Error(errors.WithStack(err)))
	}
	cancel()
}

// GetOwnerID is similar to concurrency.Election.Leader() but it doesn't need an concurrency.Election.
func (m *election) GetOwnerID(ctx context.Context) (string, error) {
	kv, err := m.getOwnerInfo(ctx)
	if err != nil {
		return "", err
	}
	return hack.String(kv.Value), nil
}

func (m *election) getOwnerInfo(ctx context.Context) (*mvccpb.KeyValue, error) {
	if m.etcdCli == nil {
		return nil, concurrency.ErrElectionNoLeader
	}
	kvs, err := etcd.GetKVs(ctx, m.etcdCli, m.key, clientv3.WithFirstCreate(), m.cfg.Timeout, m.cfg.RetryIntvl, m.cfg.RetryCnt)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, concurrency.ErrElectionNoLeader
	}
	return kvs[0], nil
}

func (m *election) watchOwner(ctx context.Context, session *concurrency.Session, key string) {
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				m.lg.Info("watcher is closed, retry watching owner")
				return
			}
			if resp.Canceled {
				m.lg.Info("watch canceled, retry watching owner")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					// The owner key may disappear before another member campaigns.
					// Keep the local owner state and let the next campaign round decide
					// whether a new owner has really taken over.
					m.lg.Info("watch found owner deleted, retry campaigning")
					return
				}
			}
		case <-session.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

// Close resigns and retires.
func (m *election) Close() {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	m.wg.Wait()
	if m.isOwner {
		m.onRetired()
	}
}
