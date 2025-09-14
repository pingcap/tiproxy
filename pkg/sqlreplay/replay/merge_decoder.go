// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"io"

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
	decoders []decoder
	buf      []*cmd.Command
}

func newMergeDecoder(decoders ...decoder) *mergeDecoder {
	return &mergeDecoder{
		decoders: decoders,
	}
}

// Decode returns the command with the smallest StartTS from multiple decoders.
func (d *mergeDecoder) Decode() (*cmd.Command, error) {
	for i, decoder := range d.decoders {
		if decoder == nil {
			continue
		}

		if i >= len(d.buf) {
			d.buf = append(d.buf, nil)
		}

		if d.buf[i] == nil {
			cmd, err := decoder.Decode()
			if err != nil {
				if err == io.EOF {
					// Mark decoder as exhausted
					d.decoders[i] = nil
					continue
				}
				return nil, err
			}
			d.buf[i] = cmd
		}
	}

	var minIdx = -1
	var minCmd *cmd.Command

	for i, cmd := range d.buf {
		if cmd != nil {
			if minCmd == nil || cmd.StartTs.Before(minCmd.StartTs) {
				minCmd = cmd
				minIdx = i
			}
		}
	}

	if minCmd == nil {
		return nil, io.EOF
	}
	d.buf[minIdx] = nil

	return minCmd, nil
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
