// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type TLSConfig struct {
	Cert               string   `yaml:"cert,omitempty" toml:"cert,omitempty" json:"cert,omitempty" reloadable:"true"`
	Key                string   `yaml:"key,omitempty" toml:"key,omitempty" json:"key,omitempty" reloadable:"true"`
	CA                 string   `yaml:"ca,omitempty" toml:"ca,omitempty" json:"ca,omitempty" reloadable:"true"`
	MinTLSVersion      string   `yaml:"min-tls-version,omitempty" toml:"min-tls-version,omitempty" json:"min-tls-version,omitempty" reloadable:"true"`
	CertAllowedCN      []string `yaml:"cert-allowed-cn,omitempty" toml:"cert-allowed-cn,omitempty" json:"cert-allowed-cn,omitempty" reloadable:"true"`
	AutoCerts          bool     `yaml:"auto-certs,omitempty" toml:"auto-certs,omitempty" json:"auto-certs,omitempty" reloadable:"true"`
	RSAKeySize         int      `yaml:"rsa-key-size,omitempty" toml:"rsa-key-size,omitempty" json:"rsa-key-size,omitempty" reloadable:"true"`
	AutoExpireDuration string   `yaml:"autocert-expire-duration,omitempty" toml:"autocert-expire-duration,omitempty" json:"autocert-expire-duration,omitempty" reloadable:"true"`
	SkipCA             bool     `yaml:"skip-ca,omitempty" toml:"skip-ca,omitempty" json:"skip-ca,omitempty" reloadable:"true"`
}

func (c TLSConfig) HasCert() bool {
	return !(c.Cert == "" && c.Key == "")
}

func (c TLSConfig) HasCA() bool {
	return c.CA != ""
}

type Security struct {
	ServerSQLTLS      TLSConfig `yaml:"server-tls,omitempty" toml:"server-tls,omitempty" json:"server-tls,omitempty"`
	ServerHTTPTLS     TLSConfig `yaml:"server-http-tls,omitempty" toml:"server-http-tls,omitempty" json:"server-http-tls,omitempty"`
	ClusterTLS        TLSConfig `yaml:"cluster-tls,omitempty" toml:"cluster-tls,omitempty" json:"cluster-tls,omitempty"`
	SQLTLS            TLSConfig `yaml:"sql-tls,omitempty" toml:"sql-tls,omitempty" json:"sql-tls,omitempty"`
	EncryptionKeyPath string    `yaml:"encryption-key-path,omitempty" toml:"encryption-key-path,omitempty" json:"encryption-key-path,omitempty" reloadable:"true"`
	RequireBackendTLS bool      `yaml:"require-backend-tls,omitempty" toml:"require-backend-tls,omitempty" json:"require-backend-tls,omitempty" reloadable:"true"`
}
