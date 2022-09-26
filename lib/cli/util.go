// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"

	"github.com/pingcap/TiProxy/lib/util/errors"
	"go.uber.org/zap"
)

type Context struct {
	Logger *zap.Logger
	Client *http.Client
	CUrls  []string
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

	var rete string
	var res *http.Response
	for _, i := range rand.Perm(len(bctx.CUrls)) {
		req.URL.Host = bctx.CUrls[i]

		res, err = bctx.Client.Do(req)
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
		resb, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()

		switch res.StatusCode {
		case http.StatusOK:
			return string(resb), nil
		case http.StatusBadRequest:
			return "", errors.Errorf("bad request: %s", string(resb))
		case http.StatusInternalServerError:
			err = errors.Errorf("internal error: %s", string(resb))
			continue
		default:
			rete = fmt.Sprintf("%s: %s", res.Status, string(resb))
			continue
		}
	}

	return rete, err
}
