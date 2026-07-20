// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
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

type fileCallback func(fileName string)

type singleDecoder struct {
	decoder       cmd.CmdDecoder
	reader        cmd.LineReader
	lg            *zap.Logger
	onDecoded     fileCallback
	markDecodeDone fileCallback
	curFile       string
	curEnd        time.Time
}

func newSingleDecoder(decoder cmd.CmdDecoder, reader cmd.LineReader, lg *zap.Logger, onDecoded, markDecodeDone fileCallback) *singleDecoder {
	return &singleDecoder{
		decoder:        decoder,
		reader:         reader,
		lg:             lg,
		onDecoded:      onDecoded,
		markDecodeDone: markDecodeDone,
	}
}

// Decode decodes a command from the single reader. FileName switches here are per-reader,
// so they genuinely mean the previous file of this reader is fully decoded.
func (d *singleDecoder) Decode() (*cmd.Command, error) {
	cmd, err := d.decoder.Decode(d.reader)
	if err != nil {
		// EOF: the last file of this reader is done.
		if errors.Is(err, io.EOF) && d.curFile != "" && d.markDecodeDone != nil {
			d.markDecodeDone(d.curFile)
			d.curFile = ""
			d.curEnd = time.Time{}
		}
		return cmd, err
	}
	if cmd == nil || cmd.FileName == "" || cmd.StartTs.IsZero() {
		return cmd, err
	}

	endTs := cmd.StartTs
	if !cmd.EndTs.IsZero() {
		endTs = cmd.EndTs
	}

	if d.curFile == "" {
		d.curFile = cmd.FileName
		d.curEnd = endTs
		if d.onDecoded != nil {
			d.onDecoded(cmd.FileName)
		}
		return cmd, nil
	}

	if cmd.FileName == d.curFile {
		if endTs.After(d.curEnd) {
			d.curEnd = endTs
		}
		if d.onDecoded != nil {
			d.onDecoded(cmd.FileName)
		}
		return cmd, nil
	}

	if overlap := d.curEnd.Sub(cmd.StartTs); overlap > time.Minute {
		d.lg.Warn("consecutive replay files have overlapping SQL timestamps",
			zap.String("prev_file", d.curFile),
			zap.String("cur_file", cmd.FileName),
			zap.Duration("overlap", overlap),
			zap.Time("prev_end", d.curEnd),
			zap.Time("cur_start", cmd.StartTs),
		)
	}

	// The previous file of this reader is fully decoded.
	prevFile := d.curFile
	d.curFile = cmd.FileName
	d.curEnd = endTs
	if d.markDecodeDone != nil {
		d.markDecodeDone(prevFile)
	}
	if d.onDecoded != nil {
		d.onDecoded(cmd.FileName)
	}
	return cmd, nil
}
