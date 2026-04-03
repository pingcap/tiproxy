// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package vip

import (
	"runtime"
	"strings"
	"syscall"
	"time"

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
	// garpBurstCount and garpBurstInterval define one takeover burst. The
	// manager may replay the whole burst later during the refresh window.
	garpBurstCount    int
	garpBurstInterval time.Duration
}

func NewNetworkOperation(addressStr, linkStr string, garpBurstCount int, garpBurstInterval time.Duration, lg *zap.Logger) (NetworkOperation, error) {
	no := &networkOperation{
		lg:                lg,
		garpBurstCount:    garpBurstCount,
		garpBurstInterval: garpBurstInterval,
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
	if no.garpBurstCount <= 0 {
		return nil
	}
	for i := 0; i < no.garpBurstCount; i++ {
		if err := no.sendARPOneShot(); err != nil {
			return err
		}
		if no.garpBurstInterval > 0 && i+1 < no.garpBurstCount {
			time.Sleep(no.garpBurstInterval)
		}
	}
	return nil
}

func (no *networkOperation) Addr() string {
	if no.address == nil {
		return ""
	}
	return no.address.IP.String()
}

func (no *networkOperation) sendARPOneShot() error {
	// Keep both sending paths for a single logical GARP attempt: the library
	// path avoids depending on the external "arping" command, while the command
	// path has proven more reliable on some customer environments. Treat either
	// one as success.
	libErr := arping.GratuitousArpOverIfaceByName(no.address.IP, no.link.Attrs().Name)
	if libErr != nil {
		no.lg.Warn("gratuitous arping via library failed",
			zap.Stringer("ip", no.address.IP),
			zap.String("iface", no.link.Attrs().Name),
			zap.Error(libErr))
	}
	// Always use "arping -c 1" here and let SendARP control the outer burst
	// count and interval. Using "arping -c 5" would hide pacing inside the
	// command, making takeover and refresh timing less predictable from TiProxy
	// and harder to reason about in tests and production troubleshooting.
	cmdErr := no.execCmd("sudo", "arping", "-c", "1", "-U", "-I", no.link.Attrs().Name, no.address.IP.String())
	if libErr == nil || cmdErr == nil {
		return nil
	}
	return errors.Wrap(errors.WithStack(cmdErr), errors.WithStack(libErr))
}

func (no *networkOperation) execCmd(args ...string) error {
	output, err := cmd.ExecCmd(args[0], args[1:]...)
	no.lg.Info("executed cmd", zap.String("cmd", strings.Join(args, " ")), zap.String("output", output), zap.Error(err))
	return err
}
