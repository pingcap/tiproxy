// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"context"
	"io"
	"reflect"
	"runtime"
	"sync"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

type fileParsedCallback func(fileName string)

// decoder is used to decode commands from one or multiple readers.
type decoder interface {
	Decode() (*cmd.Command, error)
}

type decodeResult struct {
	cmd *cmd.Command
	err error
}

// asyncDecoder reads from the inner decoder in a background goroutine so that
// mergeDecoder is not blocked when the inner decoder waits for more input.
type asyncDecoder struct {
	inner decoder
	ch    chan decodeResult
	ctx   context.Context
}

func newAsyncDecoder(ctx context.Context, inner decoder) *asyncDecoder {
	ad := &asyncDecoder{
		inner: inner,
		ch:    make(chan decodeResult, 1),
		ctx:   ctx,
	}
	go ad.run()
	return ad
}

func (ad *asyncDecoder) run() {
	defer close(ad.ch)
	for {
		if err := ad.ctx.Err(); err != nil {
			return
		}
		cmd, err := ad.inner.Decode()
		if ad.ctx.Err() != nil {
			return
		}
		select {
		case ad.ch <- decodeResult{cmd: cmd, err: err}:
		case <-ad.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// mergeDecoder merges multiple decoders and merge sort them in `StartTS` order.
// However, if the commands read from decoder are not sorted, the order is not
// strictly guaranteed.
type mergeDecoder struct {
	sync.Mutex

	ctx      context.Context
	decoders map[*asyncDecoder]struct{}
	buf      map[*asyncDecoder]*cmd.Command
}

func newMergeDecoder(ctx context.Context, decoders ...decoder) *mergeDecoder {
	decodersMap := make(map[*asyncDecoder]struct{})
	buf := make(map[*asyncDecoder]*cmd.Command)
	for _, d := range decoders {
		ad := newAsyncDecoder(ctx, d)
		decodersMap[ad] = struct{}{}
		buf[ad] = nil
	}
	return &mergeDecoder{
		ctx:      ctx,
		decoders: decodersMap,
		buf:      buf,
	}
}

// Decode returns the command with the smallest StartTS from multiple decoders.
func (d *mergeDecoder) Decode() (*cmd.Command, error) {
	d.Lock()
	defer d.Unlock()

	for {
		if err := d.ctx.Err(); err != nil {
			return nil, err
		}
		if err := d.tryFillBuffers(false); err != nil {
			return nil, err
		}
		if err := d.spinDrain(); err != nil {
			return nil, err
		}
		minIdx, minCmd := d.pickMin()
		if minCmd != nil {
			d.buf[minIdx] = nil
			return minCmd, nil
		}
		if len(d.decoders) == 0 {
			return nil, io.EOF
		}
		if err := d.waitForOne(); err != nil {
			return nil, err
		}
	}
}

func (d *mergeDecoder) tryFillBuffers(block bool) error {
	if block {
		return d.waitForOne()
	}
	for {
		filled := false
		for ad := range d.decoders {
			if d.buf[ad] != nil {
				continue
			}
			select {
			case result, ok := <-ad.ch:
				filled = true
				if err := d.applyResult(ad, result, ok); err != nil {
					return err
				}
			default:
			}
		}
		if !filled {
			return nil
		}
	}
}

func (d *mergeDecoder) applyResult(ad *asyncDecoder, result decodeResult, ok bool) error {
	if !ok {
		delete(d.decoders, ad)
		delete(d.buf, ad)
		return nil
	}
	if result.err != nil {
		delete(d.decoders, ad)
		delete(d.buf, ad)
		if errors.Is(result.err, io.EOF) {
			return nil
		}
		return result.err
	}
	d.buf[ad] = result.cmd
	return nil
}

func (d *mergeDecoder) spinDrain() error {
	for range len(d.decoders) {
		filled, err := d.drainReadyOnce()
		if err != nil {
			return err
		}
		if !filled {
			runtime.Gosched()
		}
	}
	return nil
}

func (d *mergeDecoder) drainReadyOnce() (bool, error) {
	filled := false
	for ad := range d.decoders {
		if d.buf[ad] != nil {
			continue
		}
		select {
		case result, ok := <-ad.ch:
			filled = true
			if err := d.applyResult(ad, result, ok); err != nil {
				return filled, err
			}
		default:
		}
	}
	return filled, nil
}

func (d *mergeDecoder) waitForOne() error {
	if err := d.ctx.Err(); err != nil {
		return err
	}
	var cases []reflect.SelectCase
	var ads []*asyncDecoder
	for ad := range d.decoders {
		if d.buf[ad] != nil {
			continue
		}
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ad.ch),
		})
		ads = append(ads, ad)
	}
	if len(cases) == 0 {
		return nil
	}
	ctxCaseIdx := len(cases)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(d.ctx.Done()),
	})
	chosen, recv, recvOK := reflect.Select(cases)
	if chosen == ctxCaseIdx {
		return d.ctx.Err()
	}
	ad := ads[chosen]
	if !recvOK {
		return d.applyResult(ad, decodeResult{}, false)
	}
	result := recv.Interface().(decodeResult)
	return d.applyResult(ad, result, true)
}

func (d *mergeDecoder) pickMin() (*asyncDecoder, *cmd.Command) {
	var minIdx *asyncDecoder
	var minCmd *cmd.Command
	for ad, cmd := range d.buf {
		if cmd != nil {
			// The condition `(cmd.StartTs.Equal(minCmd.StartTs) && cmd.ConnID < minCmd.ConnID)` is used to
			// have a stable order when StartTs are the same, which will help in testing.
			if minCmd == nil || cmd.StartTs.Before(minCmd.StartTs) ||
				(cmd.StartTs.Equal(minCmd.StartTs) && cmd.ConnID < minCmd.ConnID) {
				minCmd = cmd
				minIdx = ad
			}
		}
	}
	return minIdx, minCmd
}

func (d *mergeDecoder) AddDecoder(decoder decoder) {
	d.Lock()
	defer d.Unlock()

	ad := newAsyncDecoder(d.ctx, decoder)
	d.decoders[ad] = struct{}{}
	d.buf[ad] = nil
}

type singleDecoder struct {
	decoder      cmd.CmdDecoder
	reader       cmd.LineReader
	onFileParsed fileParsedCallback
	lastFileName string
}

func newSingleDecoder(decoder cmd.CmdDecoder, reader cmd.LineReader, onFileParsed fileParsedCallback) *singleDecoder {
	return &singleDecoder{
		decoder:      decoder,
		reader:       reader,
		onFileParsed: onFileParsed,
	}
}

// Decode decodes a command from the single reader.
func (d *singleDecoder) Decode() (*cmd.Command, error) {
	cmd, err := d.decoder.Decode(d.reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			d.markFileParsed(d.lastFileName)
		}
		return nil, err
	}
	if cmd != nil && cmd.FileName != "" && cmd.FileName != d.lastFileName {
		d.markFileParsed(d.lastFileName)
		d.lastFileName = cmd.FileName
	}
	return cmd, err
}

func (d *singleDecoder) markFileParsed(fileName string) {
	if fileName == "" || d.onFileParsed == nil {
		return
	}
	d.onFileParsed(fileName)
}
