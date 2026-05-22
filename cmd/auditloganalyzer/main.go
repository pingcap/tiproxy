// Copyright 2025 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/cmd"
	lg "github.com/pingcap/tiproxy/lib/util/logger"
	replaycmd "github.com/pingcap/tiproxy/pkg/sqlreplay/cmd"
	"github.com/pingcap/tiproxy/pkg/sqlreplay/replay"
	"github.com/pingcap/tiproxy/pkg/util/versioninfo"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	formatCSV   = "csv"
	formatMySQL = "mysql"
)

func main() {
	rootCmd := &cobra.Command{
		Use:     os.Args[0],
		Short:   "start the analyzer",
		Version: fmt.Sprintf("%s, commit %s", versioninfo.TiProxyVersion, versioninfo.TiProxyGitHash),
	}
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	input := rootCmd.PersistentFlags().String("input", "", "directory for traffic files")
	startTime := rootCmd.PersistentFlags().Time("start-time", time.Time{}, []string{time.RFC3339, time.RFC3339Nano}, "the start time to analyze the audit log.")
	endTime := rootCmd.PersistentFlags().Time("end-time", time.Time{}, []string{time.RFC3339, time.RFC3339Nano}, "the end time to analyze the audit log.")
	output := rootCmd.PersistentFlags().String("output", "audit_log_analysis_result.csv", "the output path for analysis result.")
	db := rootCmd.PersistentFlags().String("db", "", "the target database to analyze. Empty means all databases will be recorded.")
	filterCommandWithRetry := rootCmd.PersistentFlags().Bool("filter-command-with-retry", false, "filter out commands that are retries according to the audit log.")
	outputFormat := rootCmd.PersistentFlags().String("output-format", "csv", "the output format for analysis result. Currently only 'csv' and 'mysql' is supported.")
	outputTableName := rootCmd.PersistentFlags().String("output-table-name", "audit_log_analysis", "the output table name when output format is 'mysql'.")

	rootCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		logger, _, _, err := lg.BuildLogger(&config.Log{
			Encoder: "tidb",
			LogOnline: config.LogOnline{
				Level: "info",
			},
		})
		if err != nil {
			return err
		}

		result, err := replay.Analyze(logger, replaycmd.AnalyzeConfig{
			Input:                  *input,
			Start:                  *startTime,
			End:                    *endTime,
			DB:                     *db,
			FilterCommandWithRetry: *filterCommandWithRetry,
		})
		if err != nil {
			return err
		}

		switch *outputFormat {
		case formatCSV:
			logger.Info("writing analysis result to CSV", zap.String("output", *output))
			return writeAnalyzeResultToCSV(result, *output)
		case formatMySQL:
			logger.Info("writing analysis result to MySQL", zap.String("output", *output), zap.String("table", *outputTableName))
			return writeAnalyzeResultToMySQL(result, *output, *outputTableName)
		default:
			return fmt.Errorf("unsupported output format: %s", *outputFormat)
		}
	}

	cmd.RunRootCommand(rootCmd)
}

func writeAnalyzeResultToCSV(result replaycmd.AuditLogAnalyzeResult, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	for sql, group := range result {
		record := []string{
			sql,
			fmt.Sprintf("%d", group.ExecutionCount),
			fmt.Sprintf("%d", group.TotalCostTime.Microseconds()),
			fmt.Sprintf("%d", group.TotalAffectedRows),
			group.StmtTypes.String(),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return nil
}

func writeAnalyzeResultToMySQL(result replaycmd.AuditLogAnalyzeResult, outputPath string, outputTableName string) error {
	db, err := sql.Open("mysql", outputPath)
	if err != nil {
		return err
	}
	defer db.Close()

	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		sql_text TEXT,
		execution_count INT,
		total_cost_time BIGINT,
		total_affected_rows BIGINT,
		statement_types TEXT
	);`, outputTableName)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (sql_text, execution_count, total_cost_time, total_affected_rows, statement_types) VALUES (?, ?, ?, ?, ?)`, outputTableName)
	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for sqlText, group := range result {
		_, err = stmt.Exec(sqlText, group.ExecutionCount, group.TotalCostTime.Microseconds(), group.TotalAffectedRows, group.StmtTypes.String())
		if err != nil {
			return err
		}
	}

	return nil
}
