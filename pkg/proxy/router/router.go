package router

import (
	"errors"
	"math/rand"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	ErrNoInstanceToSelect = errors.New("no instances to route")
)

type RandomRouter struct {
	observer   *BackendObserver
	addresses  []string
	addr2Conns map[string]int
}

func NewRandomRouter(cfg *config.BackendNamespace) (*RandomRouter, error) {
	router := &RandomRouter{
		addr2Conns: make(map[string]int, 0),
	}
	observer, err := NewBackendObserver(router.onBackendChanged)
	if err != nil {
		return nil, err
	}
	router.observer = observer
	if observer == nil {
		if len(cfg.Instances) == 0 {
			return nil, ErrNoInstanceToSelect
		}
		router.SetAddresses(cfg.Instances)
	}
	return router, nil
}

func (router *RandomRouter) SetAddresses(addresses []string) {
	router.addresses = addresses
	for _, addr := range addresses {
		router.addr2Conns[addr] = 0
	}
}

func (router *RandomRouter) Route() (string, error) {
	length := len(router.addr2Conns)
	switch length {
	case 0:
		return "", ErrNoInstanceToSelect
	case 1:
		return router.addresses[0], nil
	default:
		return router.addresses[rand.Intn(length)], nil
	}
}

func (router *RandomRouter) AddConnOnAddr(addr string, num int) {
	router.addr2Conns[addr] += num
}

func (router *RandomRouter) updateBackend(addr2Conn map[string]driver.ClientConnection, addresses []string) error {
	return nil
}

func (router *RandomRouter) onBackendChanged(removed, added map[string]*BackendInfo) {
	for addr, info := range removed {
		logutil.BgLogger().Info("remove backend", zap.String("url", addr), zap.String("status", info.status.String()))
	}
	for addr := range added {
		logutil.BgLogger().Info("add backend", zap.String("url", addr))
	}
}

func (router *RandomRouter) Close() {
	router.observer.Close()
}
