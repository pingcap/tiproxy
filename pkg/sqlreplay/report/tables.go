// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package report

const (
	createDatabase = `create database if not exists tiproxy_traffic_replay`

	createFailTable = `create table if not exists tiproxy_traffic_replay.fail(
    cmd_type varchar(32),
    digest varchar(128),
    sample_stmt text,
    sample_err_msg text,
    sample_conn_id bigint,
    sample_capture_time timestamp,
    sample_replay_time timestamp,
    count bigint,
    primary key(cmd_type, digest))`
	insertFailTable = `insert into tiproxy_traffic_replay.fail(
	cmd_type, digest, sample_stmt, sample_err_msg, sample_conn_id, sample_capture_time, sample_replay_time, count)
	values(?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update count = count + ?`

	createOtherTable = `create table if not exists tiproxy_traffic_replay.other_errors(
    err_type varchar(256) primary key,
    sample_err_msg text,
    sample_replay_time timestamp,
    count bigint)`
	insertOtherTable = `insert into tiproxy_traffic_replay.other_errors(
	err_type, sample_err_msg, sample_replay_time, count)
	values(?, ?, ?, ?) on duplicate key update count = count + ?`
)
