package util

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
	CUrls  []string
	Logger *zap.Logger
	Client *http.Client
}

func DoRequest(ctx context.Context, bctx *Context, method string, url string, rd io.Reader) (string, error) {
	var sep string
	if len(url) > 0 && url[0] != '/' {
		sep = "/"
	}

	req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://localhost%s%s", sep, url), rd)
	if err != nil {
		return "", err
	}

	var rete string
	for i := range rand.Perm(len(bctx.CUrls)) {
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
		case http.StatusInternalServerError:
			rete = fmt.Sprintf("internal error: %s", string(resb))
			continue
		case http.StatusBadRequest:
			return string(resb), fmt.Errorf("bad request")
		}
	}

	return rete, nil
}
