// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/siddontang/go/hack"
)

const (
	AuthNativePassword      = "mysql_native_password"
	AuthCachingSha2Password = "caching_sha2_password"
	AuthTiDBSM3Password     = "tidb_sm3_password"
	AuthMySQLClearPassword  = "mysql_clear_password"
	AuthSocket              = "auth_socket"
	AuthTiDBSessionToken    = "tidb_session_token"
	AuthTiDBAuthToken       = "tidb_auth_token"
	AuthLDAPSimple          = "authentication_ldap_simple"
	AuthLDAPSASL            = "authentication_ldap_sasl"
)

// GenerateSalt generates a random string using ASCII characters but avoid separator character.
// Ref https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435.
func GenerateSalt(buf *[20]byte) error {
	n, err := rand.Read(buf[:])
	if err != nil {
		return errors.Wrapf(errors.WithStack(err), "failed to generate salt")
	}
	if n < len(buf) {
		return errors.Errorf("failed to generate salt, len %d", n)
	}
	for i := range buf {
		buf[i] &= 0x7f
		if buf[i] == 0 || buf[i] == '$' {
			buf[i] += 1
		}
	}
	return nil
}

func GenerateAuthResp(password, authPlugin string, salt []byte) ([]byte, error) {
	switch authPlugin {
	case AuthNativePassword:
		return scrambleNativePassword(salt, password)
	case AuthCachingSha2Password, AuthTiDBSM3Password:
		return scrambleSHA256Password(salt, password)
	case AuthMySQLClearPassword:
		return append(hack.Slice(password), 0), nil
	case AuthSocket:
		return hack.Slice(password), nil
	default:
		return nil, errors.Errorf("unsupported auth plugin %s", authPlugin)
	}
}

func scrambleSHA256Password(scramble []byte, password string) ([]byte, error) {
	if len(password) == 0 {
		return nil, nil
	}

	crypt := sha256.New()
	if _, err := crypt.Write(hack.Slice(password)); err != nil {
		return nil, err
	}
	message1 := crypt.Sum(nil)

	crypt.Reset()
	if _, err := crypt.Write(message1); err != nil {
		return nil, err
	}
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	if _, err := crypt.Write(message1Hash); err != nil {
		return nil, err
	}
	if _, err := crypt.Write(scramble); err != nil {
		return nil, err
	}
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}
	return message1, nil
}

func scrambleNativePassword(scramble []byte, password string) ([]byte, error) {
	if len(password) == 0 {
		return nil, nil
	}

	crypt := sha1.New()
	if _, err := crypt.Write(hack.Slice(password)); err != nil {
		return nil, err
	}
	stage1 := crypt.Sum(nil)

	crypt.Reset()
	if _, err := crypt.Write(stage1); err != nil {
		return nil, err
	}
	hash := crypt.Sum(nil)

	crypt.Reset()
	if _, err := crypt.Write(scramble); err != nil {
		return nil, err
	}
	if _, err := crypt.Write(hash); err != nil {
		return nil, err
	}
	scramble = crypt.Sum(nil)

	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble, nil
}
