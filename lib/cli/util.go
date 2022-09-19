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

	"go.uber.org/zap"
)

type Context struct {
	Logger *zap.Logger
	Client *http.Client
	CUrls  []string
}

func doRequest(ctx context.Context, bctx *Context, method string, url string, rd io.Reader) (string, error) {
	var sep string
	if len(url) > 0 && url[0] != '/' {
		sep = "/"
	}

	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://localhost%s%s", sep, url), rd)
	if err != nil {
		return "", err
	}

	var rete string
	for _, i := range rand.Perm(len(bctx.CUrls)) {
		req.URL.Host = bctx.CUrls[i]

		res, err := bctx.Client.Do(req)
		if err != nil {
			return "", err
		}
		resb, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()

		switch res.StatusCode {
		case http.StatusOK:
			return string(resb), nil
		case http.StatusBadRequest:
			return fmt.Sprintf("bad request: %s", string(resb)), nil
		case http.StatusInternalServerError:
			rete = fmt.Sprintf("internal error: %s", string(resb))
			continue
		default:
			rete = fmt.Sprintf("%s: %s", res.Status, string(resb))
			continue
		}
	}

	return rete, nil
}
