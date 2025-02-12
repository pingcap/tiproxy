// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

type TLSConfig struct {
	Cert               string   `yaml:"cert,omitempty" toml:"cert,omitempty" json:"cert,omitempty"`
	Key                string   `yaml:"key,omitempty" toml:"key,omitempty" json:"key,omitempty"`
	CA                 string   `yaml:"ca,omitempty" toml:"ca,omitempty" json:"ca,omitempty"`
	MinTLSVersion      string   `yaml:"min-tls-version,omitempty" toml:"min-tls-version,omitempty" json:"min-tls-version,omitempty"`
	CertAllowedCN      []string `yaml:"cert-allowed-cn,omitempty" toml:"cert-allowed-cn,omitempty" json:"cert-allowed-cn,omitempty"`
	AutoCerts          bool     `yaml:"auto-certs,omitempty" toml:"auto-certs,omitempty" json:"auto-certs,omitempty"`
	RSAKeySize         int      `yaml:"rsa-key-size,omitempty" toml:"rsa-key-size,omitempty" json:"rsa-key-size,omitempty"`
	AutoExpireDuration string   `yaml:"autocert-expire-duration,omitempty" toml:"autocert-expire-duration,omitempty" json:"autocert-expire-duration,omitempty"`
	SkipCA             bool     `yaml:"skip-ca,omitempty" toml:"skip-ca,omitempty" json:"skip-ca,omitempty"`
}

func (c TLSConfig) HasCert() bool {
	return !(c.Cert == "" && c.Key == "")
}

func (c TLSConfig) HasCA() bool {
	return c.CA != ""
}

type Security struct {
	ServerSQLTLS      TLSConfig  `yaml:"server-tls,omitempty" toml:"server-tls,omitempty" json:"server-tls,omitempty"`
	ServerHTTPTLS     TLSConfig  `yaml:"server-http-tls,omitempty" toml:"server-http-tls,omitempty" json:"server-http-tls,omitempty"`
	ClusterTLS        TLSConfig  `yaml:"cluster-tls,omitempty" toml:"cluster-tls,omitempty" json:"cluster-tls,omitempty"`
	SQLTLS            TLSConfig  `yaml:"sql-tls,omitempty" toml:"sql-tls,omitempty" json:"sql-tls,omitempty"`
	Encryption        Encryption `yaml:"encryption,omitempty" toml:"encryption,omitempty" json:"encryption,omitempty"`
	RequireBackendTLS bool       `yaml:"require-backend-tls,omitempty" toml:"require-backend-tls,omitempty" json:"require-backend-tls,omitempty"`
}

type Encryption struct {
	KeyPath string `yaml:"key-path,omitempty" toml:"key-path,omitempty" json:"key-path,omitempty"`
}
