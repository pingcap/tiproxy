// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"bytes"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"go.uber.org/zap"
)

type Capture interface {
	// Start starts the capture
	Start(cfg CaptureConfig) error
	// Stop stops the capture
	Stop()
	// Capture captures traffic
	Capture(packet []byte, startTime time.Time, connID uint64)
}

type CaptureConfig struct {
	outputDir string
	duration  time.Duration
}

var _ Capture = (*capture)(nil)

type capture struct {
	sync.Mutex
	cfg       CaptureConfig
	startTime time.Time
	endTime   time.Time
	lg        *zap.Logger
}

func NewCapture(lg *zap.Logger) *capture {
	return &capture{
		lg: lg,
	}
}

func (c *capture) Start(cfg CaptureConfig) error {
	c.Lock()
	defer c.Unlock()
	if !c.startTime.IsZero() {
		return errors.Errorf("capture already started, start time: %s", c.startTime.String())
	}

	c.cfg = cfg
	c.startTime = time.Now()
	c.endTime = c.startTime.Add(c.cfg.duration)
	return nil
}

func (c *capture) Capture(packet []byte, startTime time.Time, connID uint64) {
	c.Lock()
	if startTime.After(c.endTime) {
		c.Unlock()
		return
	}
	c.Unlock()

	// TODO: capture asynchronously
	command := cmd.NewCommand(packet, startTime, connID)
	if command == nil {
		return
	}
	// TODO: handle QUIT
	var buf bytes.Buffer
	if err := command.Encode(&buf); err != nil {
		return
	}
	// TODO: output to file
}

func (c *capture) Stop() {
	c.Lock()
	defer c.Unlock()
	c.startTime = time.Time{}
	c.endTime = time.Time{}
}
