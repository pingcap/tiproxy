package router

import (
	"errors"
	"math/rand"
)

var (
	ErrNoInstanceToSelect = errors.New("no instances to route")
)

type RandomRouter struct {
	addresses  []string
	addr2Conns map[string]int
}

func NewRandomRouter() *RandomRouter {
	return &RandomRouter{
		addr2Conns: make(map[string]int, 0),
	}
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
