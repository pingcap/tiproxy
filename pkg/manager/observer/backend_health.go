// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package observer

import "fmt"

type BackendStatus int

func (bs BackendStatus) ToScore() int {
	return statusScores[bs]
}

func (bs BackendStatus) String() string {
	status, ok := statusNames[bs]
	if !ok {
		return "unknown"
	}
	return status
}

const (
	StatusHealthy BackendStatus = iota
	StatusCannotConnect
	StatusMemoryHigh
	StatusRunSlow
	StatusSchemaOutdated
)

var statusNames = map[BackendStatus]string{
	StatusHealthy:        "healthy",
	StatusCannotConnect:  "down",
	StatusMemoryHigh:     "memory high",
	StatusRunSlow:        "run slow",
	StatusSchemaOutdated: "schema outdated",
}

var statusScores = map[BackendStatus]int{
	StatusHealthy:        0,
	StatusCannotConnect:  10000000,
	StatusMemoryHigh:     5000,
	StatusRunSlow:        5000,
	StatusSchemaOutdated: 10000000,
}

type BackendHealth struct {
	Status BackendStatus
	// The error occurred when health check fails. It's used to log why the backend becomes unhealthy.
	PingErr error
	// The backend version that returned to the client during handshake.
	ServerVersion string
}

func (bh *BackendHealth) String() string {
	str := fmt.Sprintf("status: %s", bh.Status.String())
	if bh.PingErr != nil {
		str += fmt.Sprintf(", err: %s", bh.PingErr.Error())
	}
	return str
}

// BackendInfo stores the status info of each backend.
type BackendInfo struct {
	IP         string
	StatusPort uint
}
