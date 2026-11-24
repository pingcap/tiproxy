// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactStorageURL(t *testing.T) {
	require.Equal(t, "/tmp/traffic", RedactStorageURL("/tmp/traffic"))
	require.Equal(t, "s3://bucket/in", RedactStorageURL("s3://bucket/in?access-key=AKIA&secret-access-key=SECRET&endpoint=http://user:PASS@minio:9000"))
	require.Equal(t, "s3://bucket/in", RedactStorageURL("s3://user:SECRET@bucket/in"))
	require.Equal(t, "azure://container/out", RedactStorageURL("azure://container/out?account-name=acct&account-key=KEY&sas-token=TOKEN"))
	require.Equal(t, "s3://bucket/in", RedactStorageURL("s3://bucket/in?secret-access-key=SECRET,LEAKED"))
}

func TestRedactStorageList(t *testing.T) {
	raw := "/tmp/traffic, s3://user:PASS@bucket/in?secret-access-key=SECRET,oss://bucket/in2?access-key=ACCESS"
	require.Equal(t, "/tmp/traffic,s3://bucket/in,oss://bucket/in2", RedactStorageList(raw))
}
