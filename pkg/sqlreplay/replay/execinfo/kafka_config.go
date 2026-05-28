// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"crypto/tls"
	"strings"
	"time"

	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	securityProtocolPlaintext     = "PLAINTEXT"
	securityProtocolSASLPlaintext = "SASL_PLAINTEXT"
	securityProtocolSASLSSL       = "SASL_SSL"

	saslMechanismPlain = "PLAIN"
)

// KafkaConfig configures writing exec info records to Kafka.
// Field names align with Apache Kafka client properties:
//   - security.protocol
//   - sasl.mechanism
//   - sasl.jaas.config (username/password are configured separately)
type KafkaConfig struct {
	// Brokers is a comma-separated list of broker addresses, e.g. "127.0.0.1:9092,127.0.0.1:9093".
	Brokers string `json:"brokers,omitempty"`
	Topic   string `json:"topic,omitempty"`
	// SecurityProtocol maps to security.protocol. Supported: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL.
	// Empty defaults to PLAINTEXT when no SASL credentials are set, or SASL_PLAINTEXT when username is set.
	SecurityProtocol string `json:"security.protocol,omitempty"`
	// SASLMechanism maps to sasl.mechanism. Only PLAIN is supported.
	SASLMechanism string `json:"sasl.mechanism,omitempty"`
	// SASLUsername is the username in sasl.jaas.config PlainLoginModule.
	SASLUsername string `json:"sasl.username,omitempty"`
	// SASLPassword is the password in sasl.jaas.config PlainLoginModule.
	SASLPassword string `json:"-"`
}

func (cfg KafkaConfig) Validate() error {
	if len(cfg.Topic) == 0 {
		return nil
	}
	if _, err := parseBrokers(cfg.Brokers); err != nil {
		return err
	}
	protocol, err := cfg.normalizedSecurityProtocol()
	if err != nil {
		return err
	}
	switch protocol {
	case securityProtocolPlaintext:
		if cfg.hasSASLCredentials() {
			return errors.New("sasl credentials require security.protocol SASL_PLAINTEXT or SASL_SSL")
		}
	case securityProtocolSASLPlaintext, securityProtocolSASLSSL:
		if _, err := cfg.saslMechanism(); err != nil {
			return err
		}
	default:
		return errors.Errorf("unsupported security.protocol %q", cfg.SecurityProtocol)
	}
	return nil
}

func (cfg KafkaConfig) normalizedSecurityProtocol() (string, error) {
	protocol := strings.ToUpper(strings.TrimSpace(cfg.SecurityProtocol))
	if protocol == "" {
		if cfg.hasSASLCredentials() {
			return securityProtocolSASLPlaintext, nil
		}
		return securityProtocolPlaintext, nil
	}
	switch protocol {
	case securityProtocolPlaintext, securityProtocolSASLPlaintext, securityProtocolSASLSSL:
		return protocol, nil
	default:
		return "", errors.Errorf("unsupported security.protocol %q", cfg.SecurityProtocol)
	}
}

func (cfg KafkaConfig) hasSASLCredentials() bool {
	return len(strings.TrimSpace(cfg.SASLUsername)) > 0 ||
		len(strings.TrimSpace(cfg.SASLMechanism)) > 0 ||
		len(cfg.SASLPassword) > 0
}

func (cfg KafkaConfig) saslMechanism() (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(cfg.SASLMechanism))
	if mechanism == "" {
		mechanism = saslMechanismPlain
	}
	switch mechanism {
	case saslMechanismPlain:
		username := strings.TrimSpace(cfg.SASLUsername)
		if username == "" {
			return nil, errors.New("sasl.username is required when security.protocol uses SASL")
		}
		return plain.Mechanism{
			Username: username,
			Password: cfg.SASLPassword,
		}, nil
	default:
		return nil, errors.Errorf("unsupported sasl.mechanism %q, only PLAIN is supported", cfg.SASLMechanism)
	}
}

func buildKafkaDialer(cfg KafkaConfig) (*kafka.Dialer, error) {
	protocol, err := cfg.normalizedSecurityProtocol()
	if err != nil {
		return nil, err
	}
	dialer := &kafka.Dialer{
		Timeout: 10 * time.Second,
	}
	switch protocol {
	case securityProtocolPlaintext:
		return dialer, nil
	case securityProtocolSASLPlaintext:
		dialer.SASLMechanism, err = cfg.saslMechanism()
		if err != nil {
			return nil, err
		}
	case securityProtocolSASLSSL:
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism, err = cfg.saslMechanism()
		if err != nil {
			return nil, err
		}
	}
	return dialer, nil
}
