# Proposal: Traffic-Replay

- Author(s): [djshow832](https://github.com/djshow832)
- Tracking Issue: https://github.com/pingcap/tiproxy/issues/642

## Abstract

This proposes a design of capturing traffic on the production cluster and replaying the traffic on a testing cluster to verify the SQL compatibility and performance of the new cluster.

## Background

There are some cases when users want to capture the traffic on the production cluster and replay the traffic on a testing cluster:
- A new TiDB version may have compatibility breakers, such as the statements failing, running slower, or resulting in different query results.
- When the cluster runs unexpectedly, users want to capture the traffic so that they can investigate it later by replaying the traffic.
- Test the maximum throughput of a scaled-up or scaled-down cluster using the real production workload instead of standard bench tools.

Some traffic replay tools are widely used, including tcpcopy, mysql-replay, and query-playback. Tcpcopy and mysql-replay capture data like tcpdump, while query-playback is based on slow logs. Although some of them are built for MySQL, deploying them on the proxy instance also works.

However, they have some limitations:
- Hard to deploy and use. For example, the tools may sometimes fail to be compiled, or take too long to preprocess the captured traffic files.
- Tcpcopy only captures new connections, which is unfriendly for persistent connections. Mysql-replay can capture existing connections but it loses session states such as prepared statements and session variables, which may make replay fail.
- The tools replay the traffic with one username and one current schema, which requires modification to the testing cluster.
- Tcpcopy and mysql-replay don't support TLS because they can't decode the encrypted data.
- Users need to verify the results and performance manually.

## Goals

- Verify the SQL compatibility, performance, and correctness of the new TiDB cluster and generate a report
- Capture the existing sessions even if they already have states like prepared statements
- Capture the traffic even if TLS and compression are enabled
- Provide friendly user interfaces to capture traffic, replay traffic, and check report
- Replay the traffic captured by mysql-replay to verify the compatibility of MySQL and TiDB

## Non-Goals

- Support capturing and replaying traffic on multiple active TiProxy instances
- Replicate the traffic to the new TiDB cluster online without storing the traffic
- Support capturing and replaying traffic on the TiDB Cloud

## Proposal

The basic procedure is:
1. The user creates a TiDB cluster with a new TiDB version.
2. (Optional) The user synchronizes data from the production TiDB cluster to the new cluster and then executes `ANALYZE` commands to refresh the statistics.
3. The user turns on traffic capture on the production TiDB cluster and TiProxy stores traffic to the specified directory.
4. The user turns on traffic replay on the new TiDB cluster and TiProxy replays traffic and generates a report.
5. The user checks the report to see if the result is expected.

<img src="./imgs/traffic-offline.png" alt="vip architecture" width="600">

### Traffic Capture

TiProxy stores these fields into traffic files:
- The start timestamp of this command.
- The TiProxy connection ID of this command.
- The binary format of this command.
- The first packet header, packet number, size, and duration of the response.

Most applications use persistent connections, so TiProxy should also capture existing connections. If the existing sessions are in transactions, TiProxy waits for the end of the transaction.

Existing sessions may have states like prepared statements. For these sessions, TiProxy queries the session states by `SHOW SESSION_STATES` and stores the states as `SET SESSION_STATES` statements in the traffic files.

### Traffic Storage

TiProxy is unable to capture MySQL traffic. TiProxy can write traffic files with the same format as mysql-replay, thus it can verify the compatibility of MySQL and TiDB by replaying the traffic file of mysql-replay. However, since the username, current schema, and other session states are not recorded in the traffic files of mysql-replay, TiProxy has the same limitation as mysql-replay.

It's trivial to lose traffic during failure, so TiProxy doesn't need to flush the traffic synchronously. To reduce the effect on the production cluster, TiProxy flushes the traffic in batch asynchronously with double buffering. Similarly, TiProxy loads traffic with double buffering during replaying.

The traffic files contain sensitive data, so TiProxy may need to encrypt the data.

To fully simulate the real workload, users may capture the traffic of days, so TiProxy also needs to compress data or store data on object storage systems.

### Traffic Replay

TiProxy replays traffic with the following steps:
1. Connect to TiDB using the specified username in the traffic file.
2. Replay the session states with `SET SESSION_STATES` statement in the traffic file.
3. Replay the commands one by one.

All the connections share the same username to replay. The user needs to create an account with full privilege and then specify the username and password on TiProxy.

Replaying DDL and DML statements makes the data on the testing cluster dirty, which requires another import if the replay needs to be executed again. To avoid this, TiProxy should support a statement filter, which allows read-only statements to be replayed.

When replaying commands, we support multiple speed options:
- Replay with the same speed as the production cluster. Since the command timestamps are recorded in the traffic files, TiProxy can replay with the same speed. It can simulate the production workload.
- Replay with a customized speed, such as 1.5x speed. It's used to get the maximum throughput of the new TiDB version, as well as keep the order of transactions.
- Replay as fast as possible. That is, send the next command once the previous query result is received. It can also get the maximum QPS and verify whether the execution is successful. It can't verify the correctness because it breaks transaction orders.

In case the response time exceeds the start time of the next command, TiProxy sends the next command immediately.

If the statement is too large, such as batch insert, reading the whole command at once takes too long and uses too much memory. TiProxy should read and send the commands streamingly.

### Verification and Report

TiProxy supports more kinds of comparison than existing tools:
- Compare the response packet header to verify whether the execution succeeds.
- Compare the response packet number to verify the returned row number.
- Compare the response size to verify the result set size. An alternative is to verify the checksum of the result set but it affects performance more.
- Compare the response time to verify performance degradation. The warning is reported only when degradation is too severe to avoid generating too many warnings.

The above warning types are written into 4 report files respectively. The warnings should be grouped by the statement digest because a failed statement may be reported a thousand times and hard to troubleshoot. To output the count of each statement together with the statements, the report should stay in TiProxy memory until the replay finishes.

Besides the warnings, another report file contains a summary describing the count of each kind of warning, plus the count of latency increases and decreases.

If the user only needs to verify the success of statement execution, he doesn't need to import data and TiProxy only needs to verify the packet header. Thus, the verification options should be configurable.

TiProxy stores the report into report files, the path of which is also specified.

### User Interface

To capture traffic, one needs to turn it on and then turn it off or specify a duration.

It's not intuitive to update the configuration to turn it on and off. The best way is using SQL. Since TiProxy doesn't parse SQL, TiDB parses SQL and sends the command through TiProxy HTTP API.

The HTTP API is as follows:

```http
PUT /api/traffic/capture/start?output=/tmp/traffic&duration=1h&compression=zstd&encryption=true
PUT /api/traffic/capture/stop
PUT /api/traffic/replay/start?&input=/tmp/traffic&report=/tmp/result&compression=zstd&encryption=true
PUT /api/traffic/replay/stop
# show the current progress of running catpure or replay jobs
GET /api/traffic/jobs
```

The TiProxyCtl commands:

```shell
tiproxyctl traffic capture start output=/tmp/traffic duration=1h compression=zstd encryption=true
tiproxyctl traffic capture stop
tiproxyctl traffic replay start input=/tmp/traffic report=/tmp/result compression=zstd encryption=true
tiproxyctl traffic replay stop
tiproxyctl traffic jobs
```

The SQL statements:

```SQL
ADMIN TRAFFIC CAPTURE START OUTPUT="/tmp/traffic" DURATION="1h" COMPRESSION="zstd" ENCRYPTION=true
ADMIN TRAFFIC CAPTURE STOP
ADMIN TRAFFIC REPLAY START INPUT="/tmp/traffic" COMPRESSION="zstd" ENCRYPTION=true REPORT="/tmp/result"
ADMIN TRAFFIC REPLAY STOP
ADMIN TRAFFIC JOBS
```

## Alternatives

### Replicate Traffic Online

It looks simpler to replicate traffic directly to a testing cluster:

<img src="./imgs/traffic-online.png" alt="vip architecture" width="500">

It's less flexible than the offline mode:
- It's almost impossible to replay traffic when the production cluster behaves unexpectedly because the testing cluster is not ready.
- If the test result is unexpected and the replication needs to be scheduled again, the performance of the production cluster is affected again.

## Future works

### Store the Report to TiDB

Now that TiProxy has a fully privileged account to access TiDB on the testing cluster, another choice is to store the report in TiDB tables. Showing the report in a structured format will be more straightforward and brings more advantages:
- Support operations such as filtering and aggregating
- TiProxy can output to the table streamingly to reduce memory usage

The table definitions:

```sql
create table traffic_replay.summary(
    failed_count int,
    failed_stmt_types int,
    faster_stmt_count int,
    slower_stmt_count int,
    result_mismatches int);
    
# the statements that failed to run
create table traffic_replay.fails(
    digest varchar(128),
    sample_stmt text,
    count int,
    err_msg text,
    primary key(digest));
    
# the statements that are much slower
create table traffic_replay.slow(
    digest varchar(128),
    sample_stmt text,
    count int,
    ratio float,
    primary key(digest));
    
# the statements that results mismatch
create table traffic.mismatch(
    digest varchar(128),
    sample_stmt text,
    count int,
    expected_rows int,
    actual_rows int,
    expected_size int,
    actual_size int,
    primary key(digest));
```

### Provide GUI with TiDB Dashboard

Offering GUI on the TiDB Dashboard is more friendly than providing SQL. Dashboard can send HTTP requests to TiProxy directly so it doesn't require SQL. Besides, showing the report on GUI is more straightforward. 

However, the TiDB Dashboard is unavailable on the cloud, limiting the scope. We may support it later if self-managed users really want it.