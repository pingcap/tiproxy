package proxy

import (
	"time"

	"github.com/djshow832/weir/pkg/config"
	"github.com/djshow832/weir/pkg/configcenter"
	"github.com/djshow832/weir/pkg/proxy/driver"
	"github.com/djshow832/weir/pkg/proxy/metrics"
	"github.com/djshow832/weir/pkg/proxy/namespace"
	"github.com/djshow832/weir/pkg/proxy/server"
	"github.com/djshow832/weir/pkg/proxy/sessionmgr/backend"
	"github.com/djshow832/weir/pkg/proxy/sessionmgr/client"
)

type Proxy struct {
	cfg          *config.Proxy
	svr          *server.Server
	apiServer    *HttpApiServer
	nsmgr        *namespace.NamespaceManager
	configCenter configcenter.ConfigCenter
}

func supplementProxyConfig(cfg *config.Proxy) *config.Proxy {
	if cfg.ProxyServer.SessionTimeout <= config.MIN_SESSION_TIMEOUT {
		cfg.ProxyServer.SessionTimeout = config.MIN_SESSION_TIMEOUT
	}
	if cfg.Cluster == "" {
		cfg.Cluster = config.DefaultClusterName
	}
	return cfg
}

func NewProxy(cfg *config.Proxy) *Proxy {
	return &Proxy{
		cfg: supplementProxyConfig(cfg),
	}
}

func (p *Proxy) Init() error {
	metrics.RegisterProxyMetrics(p.cfg.Cluster)
	cc, err := configcenter.CreateConfigCenter(p.cfg.ConfigCenter)
	if err != nil {
		return err
	}
	p.configCenter = cc

	nss, err := cc.ListAllNamespace()
	if err != nil {
		return err
	}
	nsmgr, err := namespace.CreateNamespaceManager(nss, namespace.BuildNamespace, namespace.DefaultAsyncCloseNamespace)
	if err != nil {
		return err
	}
	p.nsmgr = nsmgr
	driverImpl := driver.NewDriverImpl(nsmgr, client.NewClientConnectionImpl, backend.NewBackendConnManager)
	svr, err := server.NewServer(p.cfg, driverImpl)
	if err != nil {
		return err
	}
	p.svr = svr
	apiServer, err := CreateHttpApiServer(svr, nsmgr, cc, p.cfg)
	if err != nil {
		return err
	}
	p.apiServer = apiServer

	return nil
}

// TODO(eastfisher): refactor this function
func (p *Proxy) Run() error {
	go func() {
		time.Sleep(200 * time.Millisecond)
		p.apiServer.Run()
	}()
	return p.svr.Run()
}

func (p *Proxy) Close() {
	if p.apiServer != nil {
		p.apiServer.Close()
	}
	if p.svr != nil {
		p.svr.Close()
	}
}
