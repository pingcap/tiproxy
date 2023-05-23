// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"fmt"

	"github.com/pingcap/TiProxy/lib/config"
	"github.com/pingcap/TiProxy/lib/util/errors"
)

func (e *ConfigManager) GetNamespace(ctx context.Context, ns string) (config.Namespace, error) {
	e.sts.Lock()
	n, ok := e.sts.current.Namespaces[ns]
	e.sts.Unlock()
	if !ok {
		return n, fmt.Errorf("not existed namespace")
	}
	n.Name = ns
	return n, nil
}

func (e *ConfigManager) ListAllNamespace(ctx context.Context) ([]config.Namespace, error) {
	var nss []config.Namespace
	e.sts.Lock()
	for k, v := range e.sts.current.Namespaces {
		v.Name = k
		nss = append(nss, v)
	}
	e.sts.Unlock()
	return nss, nil
}

func (e *ConfigManager) SetNamespace(ctx context.Context, nsc config.Namespace) error {
	if nsc.Name == "" {
		return errors.New("namespace name can not be empty string")
	}
	e.sts.Lock()
	if e.sts.current.Namespaces == nil {
		e.sts.current.Namespaces = make(map[string]config.Namespace)
	}
	e.sts.current.Namespaces[nsc.Name] = nsc
	e.sts.Unlock()
	return nil
}

func (e *ConfigManager) DelNamespace(ctx context.Context, ns string) error {
	e.sts.Lock()
	delete(e.sts.current.Namespaces, ns)
	e.sts.Unlock()
	return nil
}
