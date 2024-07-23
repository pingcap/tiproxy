// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package httputil

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/util/errors"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

type Client struct {
	cli          *http.Client
	getTLSConfig func() *tls.Config
}

func NewHTTPClient(getTLSConfig func() *tls.Config) *Client {
	return &Client{
		cli:          http.DefaultClient,
		getTLSConfig: getTLSConfig,
	}
}

func (c *Client) SetTimeout(t time.Duration) {
	c.cli.Timeout = t
}

func Get(httpCli Client, addr, path string, b backoff.BackOff) ([]byte, error) {
	schema := "http"
	if tlsConfig := httpCli.getTLSConfig(); tlsConfig != nil {
		schema = "https"
		httpCli.cli.Transport = &http.Transport{TLSClientConfig: tlsConfig}
	}
	url := fmt.Sprintf("%s://%s%s", schema, addr, path)
	var body []byte
	err := ConnectWithRetry(func() error {
		resp, err := httpCli.cli.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return backoff.Permanent(errors.Errorf("http status %d", resp.StatusCode))
		}
		body, err = io.ReadAll(resp.Body)
		if err != nil {
			return errors.Errorf("read response body failed, url: %s, err: %s", url, err.Error())
		}
		return err
	}, b)
	return body, err
}

func ConnectWithRetry(connect func() error, b backoff.BackOff) error {
	err := backoff.Retry(func() error {
		err := connect()
		if !pnet.IsRetryableError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
	return err
}
