// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package factor

import (
	"github.com/pingcap/tiproxy/lib/config"
	"go.uber.org/zap"
)

const (
	// balanceCount4Label indicates how many connections to balance per second.
	balanceCount4Label = 1
)

var _ Factor = (*FactorLabel)(nil)

type FactorLabel struct {
	// The label name that label-based balance should be based on.
	// An empty name indicated disabling label-based balance.
	labelName string
	// The label value of this tiproxy instance.
	selfLabelVal string
	bitNum       int
}

func NewFactorLabel() *FactorLabel {
	return &FactorLabel{
		bitNum: 1,
	}
}

func (fl *FactorLabel) Name() string {
	return "label"
}

func (fl *FactorLabel) UpdateScore(backends []scoredBackend) {
	// The score will be used for CanBeRouted so don't skip updating even when only one backend.
	if len(fl.labelName) == 0 || len(fl.selfLabelVal) == 0 {
		return
	}
	for i := 0; i < len(backends); i++ {
		score := 1
		backendLabels := backends[i].GetBackendInfo().Labels
		if backendLabels != nil && backendLabels[fl.labelName] == fl.selfLabelVal {
			score = 0
		}
		backends[i].addScore(score, fl.bitNum)
	}
}

func (fl *FactorLabel) ScoreBitNum() int {
	return fl.bitNum
}

func (fl *FactorLabel) BalanceCount(from, to scoredBackend) (BalanceAdvice, float64, []zap.Field) {
	fields := []zap.Field{
		zap.String("label_key", fl.labelName),
		zap.Any("from_labels", from.GetBackendInfo().Labels),
		zap.String("self_label_value", fl.selfLabelVal),
	}
	return AdvicePositive, balanceCount4Label, fields
}

func (fl *FactorLabel) SetConfig(cfg *config.Config) {
	fl.labelName = cfg.Balance.LabelName
	if len(fl.labelName) > 0 && cfg.Labels != nil {
		fl.selfLabelVal = cfg.Labels[fl.labelName]
	}
}

func (fl *FactorLabel) CanBeRouted(score uint64) bool {
	// Label is used to isolate business resources. Never route to the backends of another business.
	return score == 0
}

func (fl *FactorLabel) Close() {
}
