// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// vipKey is the key in etcd for VIP election. The key contains the VIP address so that
	// multiple VIP can coexist.
	vipKey = "/tiproxy/vip/%s/owner"
	// sessionTTL is the session's TTL in seconds for VIP election.
	// The etcd client keeps alive every TTL/3 seconds.
	// The TTL determines the failover time so it should be short.
	sessionTTL = 3
	// Refresh GARP for a bounded window after takeover so upstream devices have
	// repeated chances to overwrite stale VIP->MAC cache entries after abnormal
	// failover, while still avoiding permanent ARP noise.
	garpRefreshInterval = 1 * time.Second
)

// servingCheckInterval is how often the VIP manager re-evaluates whether the
// instance is rejecting connections. It is a variable so tests can
// shorten it to speed up the resign/recompete transitions.
var servingCheckInterval = 1 * time.Second

type VIPManager interface {
	Start(context.Context, *clientv3.Client) error
	// SetConnRejecter configures the checker that decides whether the instance is
	// rejecting new connections. When it reports a rejection, the manager resigns
	// the VIP owner so a healthy node takes over, and re-campaigns once it stops
	// rejecting.
	SetConnRejecter(ConnRejecter)
	PreClose()
	Close()
}

// ConnRejecter reports whether the instance is currently rejecting new
// connections.
type ConnRejecter interface {
	RejectConns() (bool, string)
}

var _ VIPManager = (*vipManager)(nil)

type vipManager struct {
	mu sync.Mutex
	// closing blocks late OnElected callbacks during controlled shutdown.
	// A VIP must not be present on two nodes at the same time because upstream
	// L3 devices cache only one VIP->MAC entry; if old and new owners both answer
	// ARP, whichever reply is learned last may blackhole cross-subnet traffic.
	closing   bool
	arpCancel context.CancelFunc
	refreshWG waitgroup.WaitGroup
	operation NetworkOperation
	cfgGetter config.ConfigGetter
	election  elect.Election
	lg        *zap.Logger
	// rejecter, when set, drives the resign/recompete loop. nil keeps the
	// historical behavior of always competing for the VIP.
	rejecter ConnRejecter
	// newElection creates and starts a fresh election. It is set in Start,
	// closing over the etcd client, election parameters, and the start context
	// so they don't need to live as separate fields. Tests override it to avoid
	// a real etcd server.
	newElection func() elect.Election
	startCtx    context.Context
	startCancel context.CancelFunc
	watchWG     waitgroup.WaitGroup
}

func NewVIPManager(lg *zap.Logger, cfgGetter config.ConfigGetter) (*vipManager, error) {
	cfg := cfgGetter.GetConfig()
	if len(cfg.HA.VirtualIP) == 0 && len(cfg.HA.Interface) == 0 {
		return nil, nil
	}
	vm := &vipManager{
		cfgGetter: cfgGetter,
		lg:        lg.With(zap.String("address", cfg.HA.VirtualIP), zap.String("link", cfg.HA.Interface)),
	}
	if len(cfg.HA.VirtualIP) == 0 || len(cfg.HA.Interface) == 0 {
		vm.lg.Warn("Both address and link must be specified to enable VIP. VIP is disabled")
		return nil, nil
	}
	operation, err := NewNetworkOperation(cfg.HA.VirtualIP, cfg.HA.Interface, cfg.HA.GARPBurstCount, lg)
	if err != nil {
		vm.lg.Error("init network operation failed", zap.Error(err))
		return nil, err
	}
	vm.operation = operation
	return vm, nil
}

// SetConnRejecter configures the checker that decides whether this instance is
// rejecting new connections. It must be called before Start. When the checker
// reports a rejection, the VIP manager resigns the owner so a healthy node
// takes over, and re-campaigns once it stops rejecting.
func (vm *vipManager) SetConnRejecter(r ConnRejecter) {
	vm.rejecter = r
}

func (vm *vipManager) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.closing = false

	// This node may have bound the VIP before last failure.
	vm.delVIP(ctx)

	cfg := vm.cfgGetter.GetConfig()
	ip, port, _, err := cfg.GetIPPort()
	if err != nil {
		return err
	}

	id := net.JoinHostPort(ip, port)
	electionCfg := elect.DefaultElectionConfig(sessionTTL)
	key := fmt.Sprintf(vipKey, vm.operation.Addr())
	vm.startCtx, vm.startCancel = context.WithCancel(ctx)
	startCtx := vm.startCtx
	vm.newElection = func() elect.Election {
		e := elect.NewElection(vm.lg.Named("elect"), etcdCli, electionCfg, id, key, vm)
		e.Start(startCtx)
		return e
	}
	vm.election = vm.newElection()

	// The watcher is a no-op when there is no rejecter, preserving the
	// historical always-compete behavior.
	if vm.rejecter != nil {
		watchCtx := vm.startCtx
		vm.watchWG.RunWithRecover(func() {
			vm.watchRejecter(watchCtx)
		}, nil, vm.lg)
	}
	return nil
}

// resign drops the current election so the etcd lease is revoked and another
// node can take over. election.Close retires this member, which removes the
// VIP locally via OnRetired.
func (vm *vipManager) resign() {
	vm.mu.Lock()
	if vm.closing || vm.election == nil {
		vm.mu.Unlock()
		return
	}
	election := vm.election
	vm.election = nil
	vm.mu.Unlock()
	// election.Close synchronously invokes OnRetired, which needs vm.mu, so it
	// must run outside the lock to avoid a self-deadlock.
	vm.lg.Info("resign VIP owner because the instance rejects new connections")
	election.Close()
}

// recompete creates a new election and starts campaigning after recovery.
func (vm *vipManager) recompete() {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.closing || vm.election != nil {
		return
	}
	vm.lg.Info("recompete for VIP because the instance accepts new connections")
	vm.election = vm.newElection()
}

// watchRejecter polls the rejecter and flips the VIP ownership on transitions.
// Polling is intentional: the health signal is a simple boolean with no event
// channel, and the interval is bounded by the election TTL so failover latency
// stays comparable to an etcd session expiry. The instance never rejects at
// startup, so the initial state is assumed to be not rejecting.
func (vm *vipManager) watchRejecter(ctx context.Context) {
	ticker := time.NewTicker(servingCheckInterval)
	defer ticker.Stop()
	wasRejecting := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reject, _ := vm.rejecter.RejectConns()
			if reject == wasRejecting {
				continue
			}
			wasRejecting = reject
			if reject {
				vm.resign()
			} else {
				vm.recompete()
			}
		}
	}
}

func (vm *vipManager) OnElected() {
	vm.mu.Lock()
	// Election.Close may race with an already in-flight OnElected callback.
	// Once controlled shutdown starts, never bind the VIP again locally.
	if vm.closing {
		vm.mu.Unlock()
		vm.lg.Info("skip adding VIP because the manager is closing")
		return
	}
	vm.stopARPRefresh()
	ctx, cancel := context.WithCancel(context.Background())
	vm.arpCancel = cancel
	vm.mu.Unlock()

	if vm.addVIP(ctx) {
		vm.mu.Lock()
		if !vm.closing {
			vm.startARPRefresh(ctx)
		}
		vm.mu.Unlock()
	}
}

func (vm *vipManager) OnRetired() {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.stopARPRefresh()
	vm.delVIP(context.Background())
}

func (vm *vipManager) addVIP(ctx context.Context) bool {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return false
	}
	if hasIP {
		vm.lg.Debug("already has VIP, do nothing")
		return true
	}
	if err := vm.operation.AddIP(ctx); err != nil {
		vm.lg.Error("adding address failed", zap.Error(err))
		return false
	}
	if err := vm.operation.SendARP(ctx); err != nil {
		vm.lg.Error("broadcast ARP failed", zap.Error(err))
		// The VIP is already bound locally. Keep the later refresh loop as a
		// best-effort retry path for notifying upstream devices.
		return true
	}
	vm.lg.Info("adding VIP success")
	return true
}

func (vm *vipManager) delVIP(ctx context.Context) {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return
	}
	if !hasIP {
		vm.lg.Debug("does not have VIP, do nothing")
		return
	}
	if err := vm.operation.DeleteIP(ctx); err != nil {
		vm.lg.Error("deleting address failed", zap.Error(err))
		return
	}
	vm.lg.Info("deleting VIP success")
}

func (vm *vipManager) startARPRefresh(ctx context.Context) {
	refreshCount := vm.cfgGetter.GetConfig().HA.GARPRefreshCount
	if refreshCount <= 0 {
		return
	}
	vm.refreshWG.RunWithRecover(func() {
		ticker := time.NewTicker(garpRefreshInterval)
		defer ticker.Stop()
		// The first burst is sent synchronously by addVIP. The follow-up bursts
		// cover devices that probe or refresh neighbor state a little later than
		// the handover moment.
		for i := 0; i < refreshCount; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := vm.operation.SendARP(ctx); err != nil {
					vm.lg.Warn("refreshing GARP failed", zap.Error(err))
					return
				}
			}
		}
	}, nil, vm.lg)
}

func (vm *vipManager) stopARPRefresh() {
	cancel := vm.arpCancel
	vm.arpCancel = nil
	if cancel != nil {
		cancel()
	}
	vm.refreshWG.Wait()
}

// PreClose deletes the VIP before resigning the owner so that controlled
// shutdowns do not expose the VIP on two nodes at the same time.
func (vm *vipManager) PreClose() {
	election := vm.prepareForClose()
	vm.watchWG.Wait()
	if election != nil {
		election.Close()
	}
}

// Close resigns the owner and makes sure the VIP is removed locally.
func (vm *vipManager) Close() {
	election := vm.prepareForClose()
	vm.watchWG.Wait()
	if election != nil {
		election.Close()
	}
}

func (vm *vipManager) prepareForClose() elect.Election {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	// Drop the VIP before resigning. Letting the new owner add the VIP first is
	// unsafe on real networks because upstream devices remember only one MAC for
	// the VIP and may keep forwarding to the old node long after the overlap.
	vm.closing = true
	vm.stopARPRefresh()
	vm.delVIP(context.Background())
	// Cancel the start context so the rejecter watcher and any in-flight election
	// campaign loop stop. watchWG is waited outside the lock to avoid deadlocking
	// with a watcher callback that may be waiting for vm.mu.
	if vm.startCancel != nil {
		vm.startCancel()
		vm.startCancel = nil
	}
	election := vm.election
	vm.election = nil
	return election
}
