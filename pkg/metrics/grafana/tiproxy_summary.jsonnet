// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

local grafana = import 'grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;
local template = grafana.template;

local myNameFlag = 'DS_TEST-CLUSTER';
local myDS = '${' + myNameFlag + '}';

// A new dashboard
local newDash = dashboard.new(
  title='Test-Cluster-TiProxy-Summary',
  editable=true,
  graphTooltip='shared_crosshair',
  refresh='30s',
  time_from='now-1h',
)
.addInput(
  name=myNameFlag,
  label='test-cluster',
  type='datasource',
  pluginId='prometheus',
  pluginName='Prometheus',
)
.addTemplate(
  template.new(
    datasource=myDS,
    hide= 2,
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(pd_cluster_status, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  // Default template for tidb-cloud
  template.new(
    allValues=null,
    current=null,
    datasource=myDS,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(pd_cluster_status{k8s_cluster="$kuberentes"}, tidb_cluster)',
    refresh='time',
    regex='',
    sort=1,
    tagValuesQuery='',
  )
).addTemplate(
  // Default template for tidb-cloud
  template.new(
    allValues='.*',
    current=null,
    datasource=myDS,
    hide='',
    includeAll=true,
    label='Instance',
    multi=false,
    name='instance',
    query='label_values(tiproxy_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    regex='',
    sort=1,
    tagValuesQuery='',
  )
);

// Server row and its panels
local serverRow = row.new(collapse=true, title='Server');
local connectionP = graphPanel.new(
  title='Connection Count',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy current connection counts.',
  format='short',
  stack=true,
)
.addTarget(
  prometheus.target(
    'tiproxy_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiproxy_server_connections{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"})',
    legendFormat='total',
  )
);

local goroutineP = graphPanel.new(
  title='Goroutine Count',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy current goroutine counts.',
  format='short',
  stack=true,
)
.addTarget(
  prometheus.target(
    'go_goroutines{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tiproxy"}',
    legendFormat='{{instance}}',
  )
);

local cpuP = graphPanel.new(
  title='CPU Usage',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy CPU usage calculated with process CPU running seconds.',
  format='percentunit',
)
.addTarget(
  prometheus.target(
    'rate(process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tiproxy"}[1m])',
    legendFormat='{{instance}}',
  )
);

local memP = graphPanel.new(
  title='Memory Usage',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy process rss memory usage.TiProxy heap memory size in use.',
  format='bytes',
)
.addTarget(
  prometheus.target(
    'process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tiproxy"}',
    legendFormat='process-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'go_memory_classes_heap_objects_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tiproxy"} + go_memory_classes_heap_unused_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", job="tiproxy"}',
    legendFormat='HeapInuse-{{instance}}',
  )
);

// Query Summary
local queryRow = row.new(collapse=true, title='Query Summary');
local durationP = graphPanel.new(
  title='Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy query durations by histogram buckets with different percents.',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiproxy_session_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiproxy_session_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_session_query_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s])) / sum(rate(tiproxy_session_query_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[30s]))',
    legendFormat='avg',
  )
);

local cpsByInstP = graphPanel.new(
  title='CPS By Instance',
  datasource=myDS,
  legend_rightSide=true,
  description='TiProxy query total statistics including both successful and failed ones.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_session_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
);

local cpsByBackP = graphPanel.new(
  title='CPS by Backend',
  datasource=myDS,
  legend_rightSide=true,
  description='MySQL command statistics by backends.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_session_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (backend)',
    legendFormat='{{backend}}',
  )
);

local cpsByCMDP = graphPanel.new(
  title='CPS by CMD',
  datasource=myDS,
  legend_rightSide=true,
  description='MySQL command statistics by command type',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_session_query_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (cmd_type)',
    legendFormat='{{cmd_type}}',
  )
);

// Balance Summary
local balanceRow = row.new(collapse=true, title='Balance');
local bConnP = graphPanel.new(
  title='Backend Connections',
  datasource=myDS,
  legend_rightSide=true,
  description='Number of connections on all backends.',
  format='short',
)
.addTarget(
  prometheus.target(
    'tiproxy_balance_b_conn{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}',
    legendFormat='{{backend}}',
  )
);

local bMigCounterP = graphPanel.new(
  title='Session Migrations',
  datasource=myDS,
  legend_rightSide=true,
  description='Number of session migrations on all backends.',
  format='short',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_balance_migrate_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (from, to, migrate_res)',
    legendFormat='{{from}}-{{to}}-{{migrate_res}}',
  )
);

local bMigDurP = graphPanel.new(
  title='Session Migration Duration',
  datasource=myDS,
  legend_rightSide=true,
  description='Duration of session migrations.',
  format='s',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiproxy_balance_migrate_duration_millis_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='99',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiproxy_balance_migrate_duration_millis_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[1m])) by (le))',
    legendFormat='95',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiproxy_balance_migrate_duration_millis_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance"}[30s])) / sum(rate(tiproxy_balance_migrate_duration_millis_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}[30s]))',
    legendFormat='avg',
  )
);

// Merge together.
local panelW = 12;
local panelH = 6;
local rowW = 24;
local rowH = 1;

local rowPos = {x:0, y:0, w:rowW, h:rowH};
local leftPanelPos = {x:0, y:0, w:panelW, h:panelH};
local rightPanelPos = {x:panelW, y:0, w:panelW, h:panelH};

newDash
.addPanel(
  serverRow
  .addPanel(cpuP, gridPos=leftPanelPos)
  .addPanel(memP, gridPos=rightPanelPos)
  .addPanel(connectionP, gridPos=leftPanelPos)
  .addPanel(goroutineP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  queryRow
  .addPanel(durationP, gridPos=leftPanelPos)
  .addPanel(cpsByInstP, gridPos=rightPanelPos)
  .addPanel(cpsByBackP, gridPos=leftPanelPos)
  .addPanel(cpsByCMDP, gridPos=rightPanelPos)
  ,
  gridPos=rowPos
)
.addPanel(
  balanceRow
  .addPanel(bConnP, gridPos=leftPanelPos)
  .addPanel(bMigCounterP, gridPos=rightPanelPos)
  .addPanel(bMigDurP, gridPos=leftPanelPos)
  ,
  gridPos=rowPos
)
