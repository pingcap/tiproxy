// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package capture

import (
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	pnet "github.com/pingcap/tiproxy/pkg/proxy/net"
)

func TestStartAndStop(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	cpt := NewCapture(lg)
	packet := append([]byte{pnet.ComQuery.Byte()}, []byte("select 1")...)
	cpt.Capture(packet, time.Now(), 100)
	cpt.Start(CaptureConfig{outputDir: "./", duration: 10 * time.Second})
	cpt.Capture(packet, time.Now(), 100)
	cpt.Stop()
	cpt.Capture(packet, time.Now(), 100)
}
