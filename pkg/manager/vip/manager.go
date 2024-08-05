// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"context"
	"net"
	"sync/atomic"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/manager/elect"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// vipKey is the key in etcd for VIP election.
	vipKey = "/tiproxy/vip/owner"
	// sessionTTL is the session's TTL in seconds for VIP election.
	// The etcd client keeps alive every TTL/3 seconds.
	// The TTL determines the failover time so it should be short.
	sessionTTL = 3
)

type VIPManager interface {
	Start(context.Context, *clientv3.Client) error
	Resign()
	Close()
}

var _ VIPManager = (*vipManager)(nil)

type vipManager struct {
	operation   NetworkOperation
	cfgGetter   config.ConfigGetter
	election    elect.Election
	delOnRetire atomic.Bool
	lg          *zap.Logger
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
	vm.delOnRetire.Store(true)
	return vm, nil
}

func (vm *vipManager) Start(ctx context.Context, etcdCli *clientv3.Client) error {
	cfg := vm.cfgGetter.GetConfig()
	ip, port, _, err := cfg.GetIPPort()
	if err != nil {
		return err
	}

	id := net.JoinHostPort(ip, port)
	electionCfg := elect.DefaultElectionConfig(sessionTTL)
	election := elect.NewElection(vm.lg, etcdCli, electionCfg, id, vipKey, vm)
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

// Resign stops compaign but does not delete the VIP.
// It's called before graceful shutdown to avoid that the VIP is deleted before another member adds VIP.
func (vm *vipManager) Resign() {
	vm.delOnRetire.Store(false)
	if vm.election != nil {
		vm.election.Close()
		vm.election = nil
	}
}

// Close stops compaign and deletes the VIP.
// It's called after graceful shutdown to ensure the VIP is finally deleted.
func (vm *vipManager) Close() {
	vm.Resign()
	vm.delVIP()
}
