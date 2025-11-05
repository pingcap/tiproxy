// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

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
)

type VIPManager interface {
	Start(context.Context, *clientv3.Client) error
	PreClose()
	Close()
}

var _ VIPManager = (*vipManager)(nil)

type vipManager struct {
	operation   NetworkOperation
	cfgGetter   config.ConfigGetter
	election    elect.Election
	lg          *zap.Logger
	delOnRetire atomic.Bool
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
	operation, err := NewNetworkOperation(cfg.HA.VirtualIP, cfg.HA.Interface, lg)
	if err != nil {
		vm.lg.Error("init network operation failed", zap.Error(err))
		return nil, err
	}
	vm.operation = operation
	return vm, nil
}

func (vm *vipManager) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	// This node may have bound the VIP before last failure.
	vm.delVIP()
	vm.delOnRetire.Store(true)

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
	vm.addVIP()
}

func (vm *vipManager) OnRetired() {
	if vm.delOnRetire.Load() {
		vm.delVIP()
	}
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

// PreClose resigns the owner but doesn't delete the VIP.
// It makes use of the graceful-wait time to wait for the new owner to shorten the failover time.
func (vm *vipManager) PreClose() {
	vm.delOnRetire.Store(false)
	if vm.election != nil {
		vm.election.Close()
	}
}

// Close resigns the owner and deletes the VIP if it was the owner.
// The new owner may not be elected but we won't wait anymore.
func (vm *vipManager) Close() {
	if vm.election != nil {
		vm.election.Close()
	}
	vm.delVIP()
}
