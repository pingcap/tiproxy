// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"io"
	"sync"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
)

// decoder is used to decode commands from one or multiple readers.
type decoder interface {
	Decode() (*cmd.Command, error)
}

// mergeDecoder merges multiple decoders and merge sort them in `StartTS` order.
// However, if the commands read from decoder are not sorted, the order is not
// strictly guaranteed.
type mergeDecoder struct {
	sync.Mutex

	decoders map[decoder]struct{}
	buf      map[decoder]*cmd.Command
}

func newMergeDecoder(decoders ...decoder) *mergeDecoder {
	decodersMap := make(map[decoder]struct{})
	buf := make(map[decoder]*cmd.Command)
	for _, d := range decoders {
		decodersMap[d] = struct{}{}
		buf[d] = nil
	}
	return &mergeDecoder{
		decoders: decodersMap,
		buf:      buf,
	}
}

// Decode returns the command with the smallest StartTS from multiple decoders.
func (d *mergeDecoder) Decode() (*cmd.Command, error) {
	d.Lock()
	defer d.Unlock()

	for decoder := range d.decoders {
		if d.buf[decoder] == nil {
			cmd, err := decoder.Decode()
			if err != nil {
				if err == io.EOF {
					// Mark decoder as exhausted
					delete(d.decoders, decoder)
					continue
				}
				return nil, err
			}
			d.buf[decoder] = cmd
		}
	}

	var minIdx decoder
	var minCmd *cmd.Command

	for d, cmd := range d.buf {
		if cmd != nil {
			// The condition `(cmd.StartTs.Equal(minCmd.StartTs) && cmd.ConnID < minCmd.ConnID)` is used to
			// have a stable order when StartTs are the same, which will help in testing.
			if minCmd == nil || cmd.StartTs.Before(minCmd.StartTs) ||
				(cmd.StartTs.Equal(minCmd.StartTs) && cmd.ConnID < minCmd.ConnID) {
				minCmd = cmd
				minIdx = d
			}
		}
	}

	if minCmd == nil {
		return nil, io.EOF
	}
	d.buf[minIdx] = nil

	return minCmd, nil
}

func (d *mergeDecoder) AddDecoder(decoder decoder) {
	d.Lock()
	defer d.Unlock()

	d.decoders[decoder] = struct{}{}
	d.buf[decoder] = nil
}

type singleDecoder struct {
	decoder cmd.CmdDecoder
	reader  cmd.LineReader
}

func newSingleDecoder(decoder cmd.CmdDecoder, reader cmd.LineReader) *singleDecoder {
	return &singleDecoder{
		decoder: decoder,
		reader:  reader,
	}
}

// Decode decodes a command from the single reader.
func (d *singleDecoder) Decode() (*cmd.Command, error) {
	return d.decoder.Decode(d.reader)
}
