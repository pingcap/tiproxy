// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"io"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

var _ decoder = (*bufferedDecoder)(nil)

// bufferedDecoder is a decoder that buffers commands from another decoder.
// The returned commands are ordered by StartTs, but the order is not strictly
// guaranteed if the `reorder` happens longer than the buffer size.
type bufferedDecoder struct {
	decoder    decoder
	ignoreErrs bool

	ctx context.Context
	ch  chan *cmd.Command

	err error
}

func newBufferedDecoder(ctx context.Context, decoder decoder, bufSize int, ignoreErrs bool) *bufferedDecoder {
	bufferedDecoder := &bufferedDecoder{
		decoder:    decoder,
		ignoreErrs: ignoreErrs,
		ctx:        ctx,
		ch:         make(chan *cmd.Command, bufSize),
	}

	go bufferedDecoder.fillBuffer()
	return bufferedDecoder
}

func (d *bufferedDecoder) fillBuffer() {
	defer close(d.ch)

	for {
		cmd, err := d.decoder.Decode()
		if err != nil {
			d.err = err
			if errors.Is(err, io.EOF) {
				break
			}
			if !d.ignoreErrs {
				break
			}
			// If ignoreErrs is true, we just ignore the error and continue.
			continue
		}

		select {
		case d.ch <- cmd:
		case <-d.ctx.Done():
			return
		}
	}
}

// Decode returns the command with the smallest StartTS from the buffer.
func (d *bufferedDecoder) Decode() (*cmd.Command, error) {
	select {
	case cmd := <-d.ch:
		if cmd == nil {
			// The `fillBuffer` goroutine should have exited, reading `d.err` is safe here.
			if d.err == nil {
				return nil, d.ctx.Err()
			}
			return nil, d.err
		}
		return cmd, nil
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	}
}
