// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"runtime"
	"syscall"

	"github.com/j-keck/arping"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/util/cmd"
	"github.com/vishvananda/netlink"
	"go.uber.org/zap"
)

// NetworkOperation is the interface for adding, deleting, and broadcasting VIP.
// Extract the operations into an interface to make testing easier.
type NetworkOperation interface {
	HasIP() (bool, error)
	AddIP() error
	DeleteIP() error
	SendARP() error
	Addr() string
}

var _ NetworkOperation = (*networkOperation)(nil)

type networkOperation struct {
	// the VIP address
	address *netlink.Addr
	// the network interface
	link netlink.Link
	lg   *zap.Logger
}

func NewNetworkOperation(addressStr, linkStr string, lg *zap.Logger) (NetworkOperation, error) {
	no := &networkOperation{
		lg: lg,
	}
	if err := no.initAddr(addressStr, linkStr); err != nil {
		return nil, err
	}
	return no, nil
}

func (no *networkOperation) initAddr(addressStr, linkStr string) error {
	if runtime.GOOS != "linux" {
		return errors.New("VIP is only supported on Linux")
	}
	address, err := netlink.ParseAddr(addressStr)
	if err != nil {
		return errors.Wrapf(errors.WithStack(err), "failed to parse address '%s'", addressStr)
	}
	no.address = address
	link, err := netlink.LinkByName(linkStr)
	if err != nil {
		return errors.Wrapf(errors.WithStack(err), "failed to find network interface '%s'", linkStr)
	}
	no.link = link
	return nil
}

func (no *networkOperation) HasIP() (bool, error) {
	addresses, err := netlink.AddrList(no.link, 0)
	if err != nil {
		return false, errors.WithStack(err)
	}
	for _, addr := range addresses {
		if addr.Equal(*no.address) {
			return true, nil
		}
	}
	return false, nil
}

func (no *networkOperation) AddIP() error {
	err := netlink.AddrAdd(no.link, no.address)
	// If TiProxy is deployed by TiUP, the user that runs TiProxy only has the sudo permission.
	if err != nil && errors.Is(err, syscall.EPERM) {
		err = no.execCmd("sudo", "ip", "addr", "add", no.address.String(), "dev", no.link.Attrs().Name)
	}
	return errors.WithStack(err)
}

func (no *networkOperation) DeleteIP() error {
	err := netlink.AddrDel(no.link, no.address)
	if err != nil && errors.Is(err, syscall.EPERM) {
		err = no.execCmd("sudo", "ip", "addr", "del", no.address.String(), "dev", no.link.Attrs().Name)
	}
	return errors.WithStack(err)
}

func (no *networkOperation) SendARP() error {
	if err := arping.GratuitousArpOverIfaceByName(no.address.IP, no.link.Attrs().Name); err != nil {
		no.lg.Warn("gratuitous arping failed", zap.Stringer("ip", no.address.IP), zap.String("iface", no.link.Attrs().Name), zap.Error(err))
	}
	// GratuitousArpOverIfaceByName may not work properly even if it returns nil, so always run a command.
	err := no.execCmd("sudo", "arping", "-c", "3", "-U", "-I", no.link.Attrs().Name, no.address.IP.String())
	return errors.WithStack(err)
}

func (no *networkOperation) Addr() string {
	if no.address == nil {
		return ""
	}
	return no.address.IP.String()
}

func (no *networkOperation) execCmd(args ...string) error {
	output, err := cmd.ExecCmd(args[0], args[1:]...)
	no.lg.Info("executed cmd", zap.Any("cmd", args), zap.String("output", output), zap.Error(err))
	return err
}
