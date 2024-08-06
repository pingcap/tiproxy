// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"os/exec"
	"runtime"
	"syscall"

	"github.com/j-keck/arping"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/vishvananda/netlink"
)

// NetworkOperation is the interface for adding, deleting, and broadcasting VIP.
// Extract the operations into an interface to make testing easier.
type NetworkOperation interface {
	HasIP() (bool, error)
	AddIP() error
	DeleteIP() error
	SendARP() error
}

var _ NetworkOperation = (*networkOperation)(nil)

type networkOperation struct {
	// the VIP address
	address *netlink.Addr
	// the network interface
	link netlink.Link
}

func NewNetworkOperation(addressStr, linkStr string) (NetworkOperation, error) {
	no := &networkOperation{}
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
		err = exec.Command("sudo", "ip", "addr", "add", no.address.String(), "dev", no.link.Attrs().Name).Run()
	}
	return errors.WithStack(err)
}

func (no *networkOperation) DeleteIP() error {
	err := netlink.AddrDel(no.link, no.address)
	if err != nil && errors.Is(err, syscall.EPERM) {
		err = exec.Command("sudo", "ip", "addr", "del", no.address.String(), "dev", no.link.Attrs().Name).Run()
	}
	return errors.WithStack(err)
}

func (no *networkOperation) SendARP() error {
	return arping.GratuitousArpOverIfaceByName(no.address.IP, no.link.Attrs().Name)
}
