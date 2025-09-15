// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"container/heap"
	"context"
	"sync"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

type closableDecoder interface {
	decoder
	Close()
}

var _ decoder = (*bufferedDecoder)(nil)

// bufferedDecoder is a decoder that buffers commands from another decoder.
// The returned commands are ordered by StartTs, but the order is not strictly
// guaranteed if the `reorder` happens longer than the buffer size.
type bufferedDecoder struct {
	decoder decoder

	bufSize int
	buf     heap.Interface

	cond   *sync.Cond
	mu     *sync.Mutex
	err    error
	ctx    context.Context
	cancel func()
}

func newBufferedDecoder(ctx context.Context, decoder decoder, bufSize int) *bufferedDecoder {
	ctx, cancel := context.WithCancel(ctx)
	buf := make(cmdQueue, 0, bufSize)
	mu := &sync.Mutex{}
	bufferedDecoder := &bufferedDecoder{
		decoder: decoder,
		bufSize: bufSize,
		buf:     &buf,
		cond:    sync.NewCond(mu),
		mu:      mu,
		ctx:     ctx,
		cancel:  cancel,
	}

	go bufferedDecoder.fillBuffer()
	go func() {
		<-ctx.Done()
		bufferedDecoder.cond.Broadcast()
	}()
	return bufferedDecoder
}

func (d *bufferedDecoder) fillBuffer() {
	for {
		cmd, err := d.decoder.Decode()
		if err != nil {
			d.mu.Lock()
			d.err = err
			d.cond.Signal()
			d.mu.Unlock()
			return
		}

		d.mu.Lock()
		for d.buf.Len() >= d.bufSize && d.ctx.Err() == nil {
			d.cond.Wait()
		}

		if d.ctx.Err() != nil {
			d.mu.Unlock()
			return
		}

		heap.Push(d.buf, cmd)
		d.cond.Signal()
		d.mu.Unlock()
	}
}

// Decode returns the command with the smallest StartTS from the buffer.
func (d *bufferedDecoder) Decode() (*cmd.Command, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for d.buf.Len() == 0 && d.err == nil && d.ctx.Err() == nil {
		d.cond.Wait()
	}

	// Then either there is at least one command in the buffer, or an error occurred.
	if d.ctx.Err() != nil {
		return nil, d.ctx.Err()
	}

	if d.buf.Len() > 0 {
		cmd := heap.Pop(d.buf).(*cmd.Command)
		d.cond.Signal()
		return cmd, nil
	}

	return nil, d.err
}

func (d *bufferedDecoder) Close() {
	d.cancel()
}

type cmdQueue []*cmd.Command

func (cq cmdQueue) Len() int { return len(cq) }

func (cq cmdQueue) Less(i, j int) bool {
	return cq[i].StartTs.Before(cq[j].StartTs)
}

func (cq cmdQueue) Swap(i, j int) {
	cq[i], cq[j] = cq[j], cq[i]
}

func (cq *cmdQueue) Push(x any) {
	*cq = append(*cq, x.(*cmd.Command))
}

func (cq *cmdQueue) Pop() any {
	length := len(*cq)
	if length == 0 {
		return nil
	}

	item := (*cq)[length-1]
	(*cq)[length-1] = nil
	*cq = (*cq)[0 : length-1]
	return item
}
