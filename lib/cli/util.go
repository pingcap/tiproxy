// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"go.uber.org/zap"
)

type Context struct {
	Logger *zap.Logger
	Client *http.Client
	Host   string
	Port   int
	SSL    bool
}

func doRequest(ctx context.Context, bctx *Context, method string, url string, rd io.Reader) (string, error) {
	var sep string
	if len(url) > 0 && url[0] != '/' {
		sep = "/"
	}

	schema := "http"
	if bctx.SSL {
		schema = "https"
	}

	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s://localhost%s%s", schema, sep, url), rd)
	if err != nil {
		return "", err
	}

	req.URL.Host = net.JoinHostPort(bctx.Host, fmt.Sprintf("%d", bctx.Port))
	res, err := bctx.Client.Do(req)
	if err != nil {
		if errors.Is(err, io.EOF) {
			if req.URL.Scheme == "https" {
				req.URL.Scheme = "http"
			} else if req.URL.Scheme == "http" {
				req.URL.Scheme = "https"
			}
			// probably server did not enable TLS, try again with plain http
			// or the reverse, server enabled TLS, but we should try https
			res, err = bctx.Client.Do(req)
		}
		if err != nil {
			return "", err
		}
	}
	resb, _ := io.ReadAll(res.Body)
	res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
		return string(resb), nil
	default:
		return "", errors.Errorf("%s: %s", res.Status, string(resb))
	}
}
