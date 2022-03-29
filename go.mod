module github.com/djshow832/weir

go 1.16

require (
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37
	github.com/gin-gonic/gin v1.7.2
	github.com/go-playground/validator/v10 v10.8.0 // indirect
	github.com/goccy/go-yaml v1.8.2
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/tidb v1.1.0-beta.0.20220316031552-d981c0e06a13
	github.com/pingcap/tidb/parser v0.0.0-20220316172753-46c43febcac0
	github.com/prometheus/client_golang v1.11.0
	github.com/siddontang/go-mysql v1.1.0
	github.com/stretchr/testify v1.7.0
	github.com/ugorji/go v1.2.6 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.uber.org/zap v1.20.0
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f
)

replace github.com/siddontang/go-mysql => github.com/ibanyu/go-mysql v1.1.0
