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

Some traffic replay tools are widely used, including tcpcopy, mysql-replay, and query-playback. Mysql-replay relies on tcpdump and tcpcopy captures traffic like tcpdump, while query-playback is based on slow logs. Although some of them are built for MySQL, deploying them on the proxy instance also works.

However, they have some limitations:
- Hard to deploy and use. For example, the tools may sometimes fail to be compiled, or take too long to preprocess the captured traffic files.
- Tcpcopy only captures new connections, which is unfriendly for persistent connections. Mysql-replay can capture existing connections but it loses session states such as prepared statements and session variables, which may make replay fail.
- The tools replay the traffic within one schema, requiring huge modifications to the testing cluster.
- The tcpdump way doesn't record the result information or duration, while the slow log records the finish time of statements and breaks statement order.
- Tcpcopy and mysql-replay don't support TLS because they can't decode the encrypted data.
- Users need to verify the correctness and performance manually.

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
3. The user turns on traffic capture on the production TiDB cluster and TiProxy stores traffic to the specified directory. The username and password should be specified.
4. The user turns on traffic replay on the testing TiDB cluster and TiProxy replays traffic and generates a report.
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

`SHOW SESSION_STATES` shows all session variables, regardless of whether they have been set by the clients or not, which may miss some problems caused by changes of default session variable values. For example, a session variable has changed its default value in the new version, but TiProxy migrates the old value to the testing cluster to overwrite the default value and reports no errors. When the user migrates applications to the new version, he may find some compatibility breakers caused by the new default value.

Some commands should be filtered:
- `COM_CHANGE_USER`, which contains encrypted passwords.
- `CREATE USER`, `ALTER USER`, `SET PASSWORD`, and `GRANT` statements, which may contain user passwords.
- `SET GLOBAL tidb_cloud_storage_url`, `BACKUP`, `RESTORE`, and `IMPORT` statements, which expose cloud URLs. 

### Traffic Storage

To support adding more fields in the future, the traffic file format is similar to slow logs:

```
# Time: 2024-01-19T16:29:48.141142Z
# Query_time: 0.01
# Conn_ID: 123
# Cmd_type: 3
# Rows_sent: 1
# Result_size: 100
# Payload_len: 8
SELECT 1
```

To replay the statements in the same order, the statements should be ordered by the start time. So the field `Time` indicates the start time instead of the end time. To order by the start time, one statement must stay in memory until all the previous statements are finished. If a statement runs too long, it's logged without query result information, so the replay skips checking the result and duration.

It's trivial to lose traffic during failure, so TiProxy doesn't need to flush the traffic synchronously. To reduce the effect on the production cluster, TiProxy flushes the traffic in batch asynchronously with double buffering. Similarly, TiProxy loads traffic with double buffering during replaying. If the IO is too slow and the buffer accumulates too much, the capture should stop immediately and the status should be reported in UI and Grafana.

The traffic files contain sensitive data, so TiProxy may need to encrypt the data. The encrypt algorithm is AES-256-CTR. Besides, the files are rotated and compressed every 300MB to reduce the effect of data leak.

To fully simulate the real workload, users may capture the traffic of days, so TiProxy also needs to compress data or store data on object storage systems.

### Traffic Replay

TiProxy replays traffic with the following steps:
1. Connect to TiDB using the specified username in the traffic file.
2. Replay the session states with `SET SESSION_STATES` statement in the traffic file.
3. Replay the commands one by one.

All the connections share the same username to replay. The user needs to create an account with full privilege and then specify the username and password on TiProxy. Thus, a COM_CHANGE_USER command should be modified to change to the same user and it's just used to clear the session states.

The failed statements will be reported to the user. The command COM_STMT_EXECUTE contains the prepared statement ID but TiProxy needs to know the text, so TiProxy should maintain the mapping of prepared statements ID and text in memory. It does the following things:
- Initialize the prepared statements through the statement `SET SESSION_STATES`
- Add a prepared statement when `COM_STMT_PREPARE` succeeds
- Remove a prepared statement when `COM_STMT_CLOSE` succeeds
- Record the parameter when `COM_STMT_SEND_LONG_DATA` succeeds and clear the parameter when `COM_STMT_RESET` or `COM_STMT_EXECUTE` succeeds
- Fill the parameters into the prepared statement text when reporting the SQL text to the user

Replaying DDL and DML statements makes the data on the testing cluster dirty, which requires another import if the replay needs to be executed again. To avoid this, TiProxy should support a statement filter, which allows read-only statements to be replayed. TiProxy may not parse the statement correctly because its version decouples with the TiDB version, so it normalizes the statement and simply searches for keywords such as `SELECT`, `UNION`, and `FOR UPDATE` to judge whether the statement is read-only.

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
- Compare the response size to verify the result set size.
- Compare the checksum of the result set to verify the correctness strictly, including the row order, but it affects performance more.
- Compare the response time to verify performance degradation. The warning is reported only when degradation is too severe to avoid generating too many warnings.

Now that TiProxy has a fully privileged account to access TiDB on the testing cluster, it can store the report in TiDB tables. Showing in tables brings more advantages than storing it in files:
- Support complex operations such as filtering, aggregating, and ordering
- TiProxy can insert  a failed statement to the table and then update the execution count periodically to reduce memory usage
- It's easy to export a table into a file, but it's hard to import a file into a table

The above warning types are written into 4 tables respectively. The warnings should be grouped by the statement digest. The table definitions:

```sql
create table tiproxy_traffic_replay.summary(
    failed_count int,
    failed_stmt_types int,
    faster_stmt_count int,
    slower_stmt_count int,
    result_mismatches int);
    
# other errors, such as network errors and handshake errors
create table tiproxy_traffic_replay.other_errors(
    err_type text primary key,
    sample_err_msg text,
    sample_replay_time timestamp,
    count bigint,
 )

# the statements that failed to run
create table tiproxy_traffic_replay.fail(
    cmd_type varchar(32),
    digest varchar(128),
    sample_stmt text,
    sample_err_msg text,
    sample_conn_id bigint,
    sample_capture_time timestamp,
    sample_replay_time timestamp,
    count bigint,
    primary key(cmd_type, digest));
    
# the statements that are much slower
create table tiproxy_traffic_replay.slow(
    cmd_type varchar(32),
    digest varchar(128),
    sample_stmt text,
    sample_conn_id bigint,
    sample_capture_time timestamp,
    sample_replay_time timestamp,
    count bigint,
    ratio float,
    primary key(cmd_type, digest));
    
# the statements that results mismatch
create table tiproxy_traffic_replay.mismatch(
    cmd_type varchar(32),
    digest varchar(128),
    sample_stmt text,
    sample_conn_id bigint,
    sample_capture_time timestamp,
    sample_replay_time timestamp,
    count bigint,
    expected_rows bigint,
    actual_rows bigint,
    expected_size bigint,
    actual_size bigint,
    primary key(cmd_type, digest));
 ```

A previous replay may run before, so all the tables should be cleared before a new replay.

If the user only needs to verify the success of statement execution, he doesn't need to import data and TiProxy only needs to verify the packet header. Thus, the verification options should be configurable.

### User Interface

To capture traffic, one needs to turn it on and then turn it off or specify a duration.

It's not intuitive to update the configuration to turn it on and off. A better way is using SQL. Since TiProxy doesn't parse SQL, TiDB parses SQL and sends the command through TiProxy HTTP API.

The HTTP API is as follows:

```http
curl -X POST "localhost:3080/api/traffic/capture" -d "duration=1m&output=/tmp/traffic"
curl -X POST "localhost:3080/api/traffic/replay" -d "input=/tmp/traffic&speed=1.0&username=root&password=123456"
curl -X POST "localhost:3080/api/traffic/cancel"
curl -X GET "localhost:3080/api/traffic/show"
```

The arguments `input` and `output` specify the directory where traffic files are placed. A filename is enough for the current design, but a directory is more extensible because it can reserve more files storing metrics and traffic metadata.

The argument `show` shows:
- The current progress of the running capture or replay job if there's one running
- The last run time, final progress, and error message if a job has finished

We also support TiProxy Control to be compatible with lower versions. The TiProxy Control commands:

```shell
tiproxyctl --curls 127.0.0.1:3080 traffic capture --output="/tmp/traffic" --duration=1h encryption=true
tiproxyctl --curls 127.0.0.1:3080 traffic replay --speed=1.0 --username="u1" --password="123456" --input="/tmp/traffic" --encryption=true
tiproxyctl --curls 127.0.0.1:3080 traffic cancel
tiproxyctl --curls 127.0.0.1:3080 traffic show
```

The SQL statements:

```SQL
ADMIN TRAFFIC CAPTURE OUTPUT="/tmp/traffic" DURATION="1h" ENCRYPTION=true
ADMIN TRAFFIC REPLAY USERNAME="u1" PASSWORD="123456" INPUT="/tmp/traffic" SPEED=1.0 ENCRYPTION=true
ADMIN TRAFFIC CANCEL
ADMIN TRAFFIC SHOW
```

The capture and replay operations should be logged for audit.

## Alternatives

### Replicate Traffic Online

It looks simpler to replicate traffic directly to a testing cluster:

<img src="./imgs/traffic-online.png" alt="vip architecture" width="500">

It's less flexible than the offline mode:
- It's almost impossible to replay traffic when the production cluster behaves unexpectedly because the testing cluster is not ready.
- If the test result is unexpected and the replication needs to be scheduled again, the performance of the production cluster is affected again.

### Be Compatible with mysql-replay

In terms of the format of traffic files, TiProxy is better to be compatible with mysql-replay so that it's able to verify the compatibility of MySQL and TiDB, but the format of mysql-replay traffic files is not extensible and hard to be compatible with. It's more practicable for TiProxy to transform the tcpdump `pcap` files into its own format.

However, since the current schema and other session states are not recorded in the traffic files, TiProxy has the same limitation as other replay tools.

## Future works

### Provide GUI with TiDB Dashboard

Offering GUI on the TiDB Dashboard is more friendly than providing SQL. Dashboard can send HTTP requests to TiProxy directly so it doesn't require SQL. Besides, showing the report on GUI is more straightforward. 

However, the TiDB Dashboard is unavailable on the cloud, limiting the scope. We may support it later if self-managed users really want it.

### Authenticate with Certificates

When replaying traffic, all connections use the same username. It has 2 disadvantages:
- It can't test the privilege system where each user can only access some databases.
- It can't test resource control where each user belongs to a resource group.

A better way is to replay with the same username as the captured connections. One way is to use session tokens. However, in the current session token design, the session token has a very short lifetime, which may expire when replaying traffic. Besides, users are required to copy the session certificates to the testing cluster.

The best way is to skip authentication when TiDB recognizes that the client is TiProxy. TiUP or users should generate a TLS certificate with a common name that is in the TiDB allow list.

### Compare Cluster Info

TiProxy can compare more things:
- Prometheus metrics, such as CPU, memory, duration, and QPS
- Global variables and default session variable values
- Configurations
- SQL plans

### Update Speed Online

To test the maximum throughput of the testing cluster, users now need to update the speed for each replay and then recover the data for the next replay. It's annoying.

We can support update speed when the replay is ongoing, so that the updates can be done in only one replay and no need to recover the data.

Furthermore, TiProxy can update the speed periodically and show a table of duration and QPS for each speed. Beyond that, it can decide the idea speed and QPS.

### Capture Automatically Based on Rules

When the cluster runs improperly, the user may not notice it in time and doesn't capture the workload. It would be helpful if TiProxy could detect the exception and capture the traffic automatically.

Users can set various rules describing when to capture traffic, such as when TiDB CPU is too high, or TiDB reports too many errors. Since the success of the response is recorded, it's more convenient to troubleshoot.

### Support Multiple TiProxy Instances

In K8s deployment, there are typically multiple TiProxy instances. It's annoying to capture traffic on each TiProxy, copy all the traffic files, and then replay traffic on each TiProxy. We can support writing traffic files to object storage. Each TiProxy writes to a directory named by its address.

To replay, it's straightforward that each TiProxy chooses one directory to replay. This requires the production and testing cluster's TiProxy instance counts to be the same. If false, a hash algorithm should map the TiProxy address and connection ID of the captured traffic to one TiProxy instance.