// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package execinfo

import (
	"testing"

	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func TestKafkaConfigValidateSASLPlaintext(t *testing.T) {
	cfg := KafkaConfig{
		Brokers:          "127.0.0.1:9092",
		Topic:            "replay-sql",
		SecurityProtocol: securityProtocolSASLPlaintext,
		SASLMechanism:    saslMechanismPlain,
		SASLUsername:     "betuser",
		SASLPassword:     "secret",
	}
	require.NoError(t, cfg.Validate())

	dialer, err := buildKafkaDialer(cfg)
	require.NoError(t, err)
	require.IsType(t, plain.Mechanism{}, dialer.SASLMechanism)
	require.Equal(t, "betuser", dialer.SASLMechanism.(plain.Mechanism).Username)
	require.Equal(t, "secret", dialer.SASLMechanism.(plain.Mechanism).Password)
	require.Nil(t, dialer.TLS)
}

func TestKafkaConfigInferSASLFromUsername(t *testing.T) {
	cfg := KafkaConfig{
		Brokers:      "127.0.0.1:9092",
		Topic:        "replay-sql",
		SASLUsername: "betuser",
		SASLPassword: "secret",
	}
	require.NoError(t, cfg.Validate())

	protocol, err := cfg.normalizedSecurityProtocol()
	require.NoError(t, err)
	require.Equal(t, securityProtocolSASLPlaintext, protocol)
}

func TestKafkaConfigValidateErrors(t *testing.T) {
	err := KafkaConfig{
		Topic:            "t",
		SecurityProtocol: securityProtocolSASLPlaintext,
	}.Validate()
	require.ErrorContains(t, err, "brokers")

	err = KafkaConfig{
		Brokers:          "127.0.0.1:9092",
		Topic:            "t",
		SecurityProtocol: securityProtocolSASLPlaintext,
	}.Validate()
	require.ErrorContains(t, err, "sasl.username")

	err = KafkaConfig{
		Brokers:          "127.0.0.1:9092",
		Topic:            "t",
		SecurityProtocol: securityProtocolPlaintext,
		SASLUsername:     "u",
	}.Validate()
	require.ErrorContains(t, err, "SASL_PLAINTEXT")
}
