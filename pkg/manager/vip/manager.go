// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// vipKey is the key in etcd for VIP election.
	vipKey = "vip"
	// sessionTTL is the session's TTL in seconds for VIP election.
	sessionTTL = 5
)

type VIPManager interface {
	Start(context.Context, *clientv3.Client) error
	OnElected()
	OnRetired()
	Close()
}

var _ VIPManager = (*vipManager)(nil)

type vipManager struct {
	operation NetworkOperation
	cfgGetter config.ConfigGetter
	election  elect.Election
	lg        *zap.Logger
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
	operation, err := NewNetworkOperation(cfg.HA.VirtualIP, cfg.HA.Interface)
	if err != nil {
		vm.lg.Error("init network operation failed", zap.Error(err))
		return nil, err
	}
	vm.operation = operation
	return vm, nil
}

func getID(cfg *config.Config) (string, error) {
	return cfg.HA.VirtualIP, nil
}

func (vm *vipManager) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	cfg := vm.cfgGetter.GetConfig()
	addr, err := getID(cfg)
	if err != nil {
		return err
	}

	electionCfg := elect.DefaultElectionConfig(sessionTTL)
	election := elect.NewElection(vm.lg, etcdCli, electionCfg, addr, vipKey, vm)
	vm.election = election
	// Check the ownership at startup just in case the node is just down and restarted.
	// Before it was down, it may be either the owner or not.
	if election.IsOwner() {
		vm.OnElected()
	} else {
		vm.OnRetired()
	}
	vm.election.Start(ctx)
	return nil
}

func (vm *vipManager) OnElected() {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return
	}
	if hasIP {
		vm.lg.Info("already has VIP, do nothing")
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

func (vm *vipManager) OnRetired() {
	hasIP, err := vm.operation.HasIP()
	if err != nil {
		vm.lg.Error("checking addresses failed", zap.Error(err))
		return
	}
	if !hasIP {
		vm.lg.Info("does not have VIP, do nothing")
		return
	}
	if err := vm.operation.DeleteIP(); err != nil {
		vm.lg.Error("deleting address failed", zap.Error(err))
		return
	}
	vm.lg.Info("deleting VIP success")
}

func (vm *vipManager) Close() {
	// The OnRetired() will be called when the election is closed.
	if vm.election != nil {
		vm.election.Close()
	}
}
