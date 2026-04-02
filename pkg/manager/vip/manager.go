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
	// Refresh GARP for a short window after takeover so upstream devices have
	// several chances to update the VIP neighbor entry.
	garpRefreshRounds = 10
)

type VIPManager interface {
	Start(context.Context, *clientv3.Client) error
	PreClose()
	Close()
}

var _ VIPManager = (*vipManager)(nil)

type vipManager struct {
	mu            sync.Mutex
	closing       bool
	refreshWG     sync.WaitGroup
	refreshCancel context.CancelFunc
	operation     NetworkOperation
	cfgGetter     config.ConfigGetter
	election      elect.Election
	lg            *zap.Logger
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
	operation, err := NewNetworkOperation(cfg.HA.VirtualIP, cfg.HA.Interface, cfg.HA.GARPBurstCount, cfg.HA.GARPBurstInterval, lg)
	if err != nil {
		vm.lg.Error("init network operation failed", zap.Error(err))
		return nil, err
	}
	vm.operation = operation
	return vm, nil
}

func (vm *vipManager) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	vm.closing = false

	// This node may have bound the VIP before last failure.
	vm.delVIP()

	cfg := vm.cfgGetter.GetConfig()
	ip, port, _, err := cfg.GetIPPort()
	if err != nil {
		return err
	}

	id := net.JoinHostPort(ip, port)
	electionCfg := elect.DefaultElectionConfig(sessionTTL)
	key := fmt.Sprintf(vipKey, vm.operation.Addr())
	vm.election = elect.NewElection(vm.lg.Named("elect"), etcdCli, electionCfg, id, key, vm)
	vm.election.Start(ctx)
	return nil
}

func (vm *vipManager) OnElected() {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.closing {
		vm.lg.Info("skip adding VIP because the manager is closing")
		return
	}
	vm.addVIP()
	vm.startARPRefresh()
}

func (vm *vipManager) OnRetired() {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.stopARPRefresh()
	vm.delVIP()
}

func (vm *vipManager) addVIP() {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return
	}
	if hasIP {
		vm.lg.Debug("already has VIP, do nothing")
		return
	}
	if err := vm.operation.AddIP(); err != nil {
		vm.lg.Error("adding address failed", zap.Error(err))
		return
	}
	if err := vm.operation.SendARP(); err != nil {
		vm.lg.Error("broadcast ARP failed", zap.Error(err))
		return
	}
	vm.lg.Info("adding VIP success")
}

func (vm *vipManager) delVIP() {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return
	}
	if !hasIP {
		vm.lg.Debug("does not have VIP, do nothing")
		return
	}
	if err := vm.operation.DeleteIP(); err != nil {
		vm.lg.Error("deleting address failed", zap.Error(err))
		return
	}
	vm.lg.Info("deleting VIP success")
}

func (vm *vipManager) startARPRefresh() {
	refreshInterval := vm.cfgGetter.GetConfig().HA.GARPRefreshInterval
	if refreshInterval <= 0 {
		return
	}
	vm.stopARPRefresh()

	ctx, cancel := context.WithCancel(context.Background())
	vm.refreshCancel = cancel
	vm.refreshWG.Add(1)
	go func() {
		defer vm.refreshWG.Done()

		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()
		for i := 0; i < garpRefreshRounds; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := vm.operation.SendARP(); err != nil {
					vm.lg.Warn("refreshing GARP failed", zap.Error(err))
					return
				}
			}
		}
	}()
}

func (vm *vipManager) stopARPRefresh() {
	cancel := vm.refreshCancel
	vm.refreshCancel = nil
	if cancel != nil {
		cancel()
	}
	vm.refreshWG.Wait()
}

// PreClose deletes the VIP before resigning the owner so that controlled
// shutdowns do not expose the VIP on two nodes at the same time.
func (vm *vipManager) PreClose() {
	election := vm.prepareForClose()
	if election != nil {
		election.Close()
	}
}

// Close resigns the owner and makes sure the VIP is removed locally.
func (vm *vipManager) Close() {
	election := vm.prepareForClose()
	if election != nil {
		election.Close()
	}

	vm.mu.Lock()
	vm.stopARPRefresh()
	vm.delVIP()
	vm.mu.Unlock()
}

func (vm *vipManager) prepareForClose() elect.Election {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.closing = true
	vm.stopARPRefresh()
	vm.delVIP()
	return vm.election
}
