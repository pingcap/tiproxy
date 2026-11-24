// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

import (
	"net/url"
	"strings"
)

// RedactStorageList keeps the scheme, host, and path of one storage URL or each
// entry in a comma-separated list. Query parameters and userinfo are removed.
// Each entry is handled on its own so a leading local path does not hide a later URL.
func RedactStorageList(raw string) string {
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ",")
	for i, part := range parts {
		parts[i] = RedactStorageURL(strings.TrimSpace(part))
	}
	return strings.Join(parts, ",")
}

// RedactStorageURL removes query parameters, fragments, and userinfo from one storage URL.
func RedactStorageURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if i := strings.IndexAny(raw, "?#"); i >= 0 {
		raw = raw[:i]
	}
	if !strings.Contains(raw, "://") && !strings.Contains(raw, "@") {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid URL)"
	}
	u.User = nil
	return u.String()
}
