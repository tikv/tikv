import os
import sys

sys.path.append(os.path.dirname(__file__))

from common import (
    DATASOURCE,
    DATASOURCE_INPUT,
    Layout,
    expr_avg,
    expr_count_rate,
    expr_histogram_avg,
    expr_histogram_quantile,
    expr_max,
    expr_max_rate,
    expr_min,
    expr_operator,
    expr_simple,
    expr_sum,
    expr_sum_aggr_over_time,
    expr_sum_delta,
    expr_sum_increase,
    expr_sum_rate,
    expr_topk,
    graph_legend,
    graph_panel,
    graph_panel_histogram_quantiles,
    heatmap_panel,
    heatmap_panel_graph_panel_histogram_quantile_pairs,
    series_override,
    stat_panel,
    target,
    template,
    yaxes,
    yaxis,
)
from grafanalib import formatunits as UNITS
from grafanalib.core import (
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    HIDE_VARIABLE,
    NULL_AS_NULL,
    SHOW,
    Dashboard,
    GraphThreshold,
    RowPanel,
    StatValueMappingItem,
    StatValueMappings,
    Templating,
)

#### Metrics Definition Start ####


def Templates() -> Templating:
    return Templating(
        list=[
            template(
                name="k8s_cluster",
                query="label_values(tikv_engine_block_cache_size_bytes, k8s_cluster)",
                data_source=DATASOURCE,
                hide=HIDE_VARIABLE,
            ),
            template(
                name="tidb_cluster",
                query='label_values(tikv_engine_block_cache_size_bytes{k8s_cluster ="$k8s_cluster"}, tidb_cluster)',
                data_source=DATASOURCE,
                hide=HIDE_VARIABLE,
            ),
            template(
                name="db",
                query='label_values(tikv_engine_block_cache_size_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, db)',
                data_source=DATASOURCE,
                hide=SHOW,
                multi=True,
                include_all=True,
            ),
            template(
                name="command",
                query='query_result(tikv_storage_command_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"} != 0)',
                data_source=DATASOURCE,
                hide=SHOW,
                regex='/\\btype="([^"]+)"/',
                multi=True,
                include_all=True,
            ),
            template(
                name="instance",
                query='label_values(tikv_engine_size_bytes{k8s_cluster ="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
                data_source=DATASOURCE,
                hide=SHOW,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="titan_db",
                query='label_values(tikv_engine_titandb_num_live_blob_file{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, db)',
                data_source=DATASOURCE,
                hide=HIDE_VARIABLE,
                multi=True,
                include_all=True,
            ),
        ]
    )


def Duration() -> RowPanel:
    layout = Layout(title="Duration")
    layout.row(
        [
            graph_panel(
                title="Write Pipeline Duration",
                description="Write Pipeline Composition",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_append_log_duration_seconds"
                        ),
                        legend_format="Write Raft Log .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_request_wait_time_duration_secs",
                        ),
                        legend_format="Propose Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_apply_wait_time_duration_secs"
                        ),
                        legend_format="Apply Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_commit_log_duration_seconds"
                        ),
                        legend_format="Replicate Raft Log .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_apply_log_duration_seconds"
                        ),
                        legend_format="Apply Duration .99",
                    ),
                ],
            ),
            graph_panel(
                title="Cop Read Duration",
                description="Read Duration Composition",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_storage_engine_async_request_duration_seconds",
                            ['type="snapshot"'],
                        ),
                        legend_format="Get Snapshot .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_coprocessor_request_wait_seconds",
                            ['type="all"'],
                        ),
                        legend_format="Cop Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.95, "tikv_coprocessor_request_handle_seconds"
                        ),
                        legend_format="Cop Handle .99",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Cluster() -> RowPanel:
    layout = Layout(title="Cluster")
    layout.row(
        [
            graph_panel(
                title="Store size",
                description="The storage size per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes",
                            label_selectors=['type = "used"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Available size",
                description="The available capacity size of each TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes",
                            label_selectors=['type="available"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Capacity size",
                description="The capacity size per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes",
                            label_selectors=['type="capacity"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="CPU",
                description="The CPU usage of each TiKV instance",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "process_cpu_seconds_total",
                            label_selectors=['job=~".*tikv"'],
                        ),
                    ),
                    target(
                        expr=expr_simple(
                            "tikv_server_cpu_cores_quota",
                            label_selectors=['job=~".*tikv"'],
                        ),
                        legend_format=r"quota-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Memory",
                description="The memory usage per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "process_resident_memory_bytes",
                            label_selectors=['job=~".*tikv"'],
                        ),
                    ),
                    target(
                        expr=expr_simple(
                            "tikv_server_memory_quota_bytes",
                            label_selectors=['job=~".*tikv"'],
                        ),
                        legend_format=r"quota-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="IO utilization",
                description="The I/O utilization per TiKV instance",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "node_disk_io_time_seconds_total",
                        ),
                        legend_format=r"{{instance}}-{{device}}",
                    ),
                ],
            ),
            graph_panel(
                title="MBps",
                description="The total bytes of read and write in each TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=['type="wal_file_bytes"'],
                        ),
                        legend_format=r"{{instance}}-write",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=['type=~"bytes_read|iter_bytes_read"'],
                        ),
                        legend_format=r"{{instance}}-read",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="QPS",
                description="The number of leaders on each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_duration_seconds_count",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["instance", "type"],
                        ),
                        legend_format=r"{{instance}}-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Errps",
                description="The total number of the gRPC message failures",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_fail_total",
                            label_selectors=['type!="kv_gc"'],
                        ),
                        legend_format=r"{{instance}}-grpc-msg-fail",
                    ),
                    target(
                        expr=expr_sum_delta(
                            "tikv_pd_heartbeat_message_total",
                            label_selectors=['type="noop"'],
                        ).extra(extra_expr="< 1"),
                        legend_format=r"{{instance}}-pd-heartbeat",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_critical_error_total",
                            by_labels=["instance", "type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Leader",
                description="The number of leaders on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_region_count",
                            label_selectors=['type="leader"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Region",
                description="The number of Regions and Buckets on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_region_count",
                            label_selectors=['type="region"'],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_region_count",
                            label_selectors=['type="buckets"'],
                        ),
                        legend_format=r"{{instance}}-buckets",
                        hide=True,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Uptime",
                description="TiKV uptime since the last restart",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_operator(
                            "time()",
                            "-",
                            expr_simple(
                                "process_start_time_seconds",
                                label_selectors=['job=~".*tikv"'],
                            ),
                        ),
                        legend_format=r"{{instance}}",
                    ),
                ],
            )
        ]
    )
    return layout.row_panel


def Errors() -> RowPanel:
    layout = Layout(title="Errors")
    layout.row(
        [
            graph_panel(
                title="Critical error",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_critical_error_total",
                            by_labels=["instance", "type"],
                        ),
                    ),
                ],
                thresholds=[GraphThreshold(value=0.0)],
            )
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Server is busy",
                description="""
Indicates occurrences of events that make the TiKV instance unavailable
temporarily, such as Write Stall, Channel Full, Scheduler Busy, and Coprocessor
Full""",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_too_busy_total",
                        ),
                        legend_format=r"scheduler-{{instance}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_channel_full_total",
                            by_labels=["instance", "type"],
                        ),
                        legend_format=r"channelfull-{{instance}}-{{type}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_request_error",
                            label_selectors=['type="full"'],
                        ),
                        legend_format=r"coprocessor-{{instance}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_stall",
                            label_selectors=[
                                'type="write_stall_percentile99"',
                                'db=~"$db"',
                            ],
                            by_labels=["instance", "db"],
                        ),
                        legend_format=r"stall-{{instance}}-{{db}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_store_write_msg_block_wait_duration_seconds_count",
                        ),
                        legend_format=r"store-write-channelfull-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Server report failures",
                description="The total number of reporting failure messages",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_server_report_failure_msg_total",
                            by_labels=["type", "instance", "store_id"],
                        ),
                        legend_format=r"{{instance}}-{{type}}-to-{{store_id}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raftstore error",
                description="The number of different raftstore errors on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_engine_async_request_total",
                            label_selectors=['status!~"success|all"'],
                            by_labels=["instance", "status"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler error",
                description="The number of scheduler errors per type on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_stage_total",
                            label_selectors=['stage=~"snapshot_err|prepare_write_err"'],
                            by_labels=["instance", "stage"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Coprocessor error",
                description="The number of different coprocessor errors on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_request_error",
                            by_labels=["instance", "reason"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="gRPC message error",
                description="The number of gRPC message errors per type on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_fail_total",
                            by_labels=["instance", "type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Leader drop",
                description="The count of dropped leaders per TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_raftstore_region_count",
                            label_selectors=['type="leader"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Leader missing",
                description="The count of missing leaders per TiKV instance",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_leader_missing",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Damaged files",
                description="RocksDB damaged SST files",
                targets=[
                    target(
                        expr=expr_simple("tikv_rocksdb_damaged_files"),
                        legend_format=r"{{instance}}-existed",
                    ),
                    target(
                        expr=expr_simple("tikv_rocksdb_damaged_files_deleted"),
                        legend_format=r"{{instance}}-deleted",
                    ),
                ],
            ),
            graph_panel(
                title="Log Replication Rejected",
                description="The count of Log Replication Reject caused by follower memory insufficient",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_server_raft_append_rejects",
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Server() -> RowPanel:
    layout = Layout(title="Server")
    layout.row(
        [
            graph_panel(
                title="CF size",
                description="The size of each column family",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum("tikv_engine_size_bytes", by_labels=["type"]),
                    ),
                ],
            ),
            graph_panel(
                title="Channel full",
                description="The total number of channel full errors on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_channel_full_total", by_labels=["instance", "type"]
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Active written leaders",
                description="The number of leaders being written on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_region_written_keys_count",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Approximate region size",
                metric="tikv_raftstore_region_size_bucket",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
            graph_panel_histogram_quantiles(
                title="Approximate region size",
                description="The approximate Region size",
                metric="tikv_raftstore_region_size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Region written bytes",
                metric="tikv_region_written_bytes_bucket",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
            graph_panel(
                title="Region average written bytes",
                description="The average rate of writing bytes to Regions per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_histogram_avg("tikv_region_written_bytes"),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Region written keys",
                metric="tikv_region_written_keys_bucket",
            ),
            graph_panel(
                title="Region average written keys",
                description="The average rate of written keys to Regions per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_histogram_avg("tikv_region_written_keys"),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Written key size",
                description="The key size for every put of apply worker",
                metric="tikv_raftstore_apply_key_size_bucket",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
            heatmap_panel(
                title="Written value size for every put of apply worker",
                metric="tikv_raftstore_apply_value_size_bucket",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Hibernate Peers",
                description="The number of peers in hibernated state",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_hibernated_peer_state",
                            by_labels=["instance", "state"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Memory trace",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_server_mem_trace_sum",
                            label_selectors=['name=~"raftstore-.*"'],
                        ),
                        legend_format="{{instance}}-{{name}}",
                    ),
                    target(
                        expr=expr_simple(
                            "raft_engine_memory_usage",
                        ),
                        legend_format="{{instance}}-raft-engine",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft Entry Cache Evicts",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raft_entries_evict_bytes",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Resolve address duration",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_address_resolve_duration_secs",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="99% Thread Pool Schedule Wait Duration",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_yatp_pool_schedule_wait_duration",
                            by_labels=["name", "priority"],
                        ),
                        legend_format="{{name}}-{{priority}}",
                    ),
                ],
                thresholds=[GraphThreshold(value=1.0)],
            ),
            graph_panel(
                title="Average Thread Pool Schedule Wait Duration",
                description="The average rate of written keys to Regions per TiKV instance",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_histogram_avg(
                            "tikv_yatp_pool_schedule_wait_duration",
                            by_labels=["name", "priority"],
                        ),
                        legend_format="{{name}}-{{priority}}",
                    ),
                ],
                thresholds=[GraphThreshold(value=1.0)],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Disk IO time per second",
                yaxes=yaxes(left_format=UNITS.NANO_SECONDS),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_rocksdb_perf",
                            label_selectors=['metric="block_read_time"'],
                            by_labels=["req"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            label_selectors=['metric="block_read_time"'],
                            by_labels=["req"],
                        ),
                        legend_format="copr-{{req}}",
                    ),
                ],
            ),
            graph_panel(
                title="Disk IO bytes per second",
                yaxes=yaxes(left_format=UNITS.NANO_SECONDS),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_rocksdb_perf",
                            label_selectors=['metric="block_read_byte"'],
                            by_labels=["req"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            label_selectors=['metric="block_read_byte"'],
                            by_labels=["req"],
                        ),
                        legend_format="copr-{{req}}",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def gRPC() -> RowPanel:
    layout = Layout(title="gRPC")
    layout.row(
        [
            graph_panel(
                title="gRPC message count",
                description="The count of different kinds of gRPC message",
                yaxes=yaxes(left_format=UNITS.REQUESTS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_duration_seconds_count",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_duration_seconds_count",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["type", "priority"],
                        ),
                        hide=True,
                    ),
                ],
            ),
            graph_panel(
                title="gRPC message failed",
                description="The count of different kinds of gRPC message which is failed",
                yaxes=yaxes(left_format=UNITS.REQUESTS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_fail_total",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title=r"99% gRPC message duration",
                description=r"The 99% percentile of execution time of gRPC message",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_grpc_msg_duration_seconds",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_grpc_msg_duration_seconds",
                            label_selectors=['type!="kv_gc"'],
                            by_labels=["type", "priority"],
                        ),
                        legend_format="{{type}}-{{priority}}",
                        hide=True,
                    ),
                ],
            ),
            graph_panel(
                title="Average gRPC message duration",
                description="The average execution time of gRPC message",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_histogram_avg(
                            "tikv_grpc_msg_duration_seconds",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_grpc_msg_duration_seconds",
                            by_labels=["type", "priority"],
                        ),
                        legend_format="{{type}}-{{priority}}",
                        hide=True,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="gRPC batch size",
                description=r"The 99% percentile of execution time of gRPC message",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_grpc_req_batch_size",
                        ),
                        legend_format=r"99% request",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_grpc_resp_batch_size",
                        ),
                        legend_format=r"99% response",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_server_grpc_req_batch_size",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg request",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_server_grpc_resp_batch_size",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg response",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_request_batch_size",
                        ),
                        legend_format=r"99% kv get batch",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_server_request_batch_size",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg kv batch",
                    ),
                ],
            ),
            graph_panel(
                title="raft message batch size",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_raft_message_batch_size",
                        ),
                        legend_format=r"99%",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_server_raft_message_batch_size",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="gRPC request sources QPS",
                description="The QPS of different sources of gRPC request",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_request_source_counter_vec",
                            by_labels=["source"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="gRPC request sources duration",
                description="The duration of different sources of gRPC request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_request_source_duration_vec",
                            by_labels=["source"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="gRPC resource group QPS",
                description="The QPS of different resource groups of gRPC request",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_resource_group_total", by_labels=["name"]
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def ThreadCPU() -> RowPanel:
    layout = Layout(title="Thread CPU")
    layout.row(
        [
            graph_panel(
                title="Raft store CPU",
                description="The CPU utilization of raftstore thread",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"(raftstore|rs)_.*"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Async apply CPU",
                description="The CPU utilization of async apply",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"apply_[0-9]+"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Store writer CPU",
                description="The CPU utilization of store writer thread",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"store_write.*"'],
                        ),
                    ),
                ],
                thresholds=[GraphThreshold(value=0.8)],
            ),
            graph_panel(
                title="gRPC poll CPU",
                description="The CPU utilization of gRPC",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"grpc.*"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Scheduler worker CPU",
                description="The CPU utilization of scheduler worker",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sched_.*"'],
                        ),
                    ),
                ],
                thresholds=[GraphThreshold(value=3.6)],
            ),
            graph_panel(
                title="Unified read pool CPU",
                description="The CPU utilization of the unified read pool",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"unified_read_po.*"'],
                        ),
                    ),
                ],
                thresholds=[GraphThreshold(value=7.2)],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="RocksDB CPU",
                description="The CPU utilization of RocksDB",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"rocksdb.*"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="GC worker CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"gc_worker.*"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Region worker CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"region_worker.*"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Snap generator CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"snap_generator.*"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Background worker CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"background.*"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raftlog fetch worker CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"raftlog_fetch.*"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Import CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sst_.*"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Backup CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"(backup-worker|bkwkr|backup_endpoint).*"'
                            ],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="CDC worker CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"cdcwkr.*"'],
                        ),
                        legend_format="{{instance}}-worker",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"tso"'],
                        ),
                        legend_format="{{instance}}-tso",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"cdc_.*"'],
                        ),
                        legend_format="{{instance}}-endpoint",
                    ),
                ],
            ),
            graph_panel(
                title="TSO worker CPU",
                description="The CPU utilization of raftstore thread",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"tso_worker"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Storage read pool CPU",
                description="The CPU utilization of storage read pool",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"store_read_norm.*"'],
                        ),
                        legend_format="{{instance}}-normal",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"store_read_high.*"'],
                        ),
                        legend_format="{{instance}}-high",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"store_read_low.*"'],
                        ),
                        legend_format="{{instance}}-low",
                    ),
                ],
                thresholds=[GraphThreshold(value=3.6)],
            ),
            graph_panel(
                title="Coprocessor read pool CPU",
                description="The CPU utilization of coprocessor read pool",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"cop_normal.*"'],
                        ),
                        legend_format="{{instance}}-normal",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"cop_high.*"'],
                        ),
                        legend_format="{{instance}}-high",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"cop_low.*"'],
                        ),
                        legend_format="{{instance}}-low",
                    ),
                ],
                thresholds=[GraphThreshold(value=7.2)],
            ),
        ]
    )
    return layout.row_panel


def TTL() -> RowPanel:
    layout = Layout(title="TTL")
    layout.row(
        [
            graph_panel(
                title="TTL expire count",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_ttl_expire_kv_count_total",
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="TTL expire size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_ttl_expire_kv_size_total",
                        )
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="TTL check progress",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_ttl_checker_processed_regions",
                            ),
                            "/",
                            expr_sum_rate(
                                "tikv_raftstore_region_count",
                                label_selectors=['type="region"'],
                            ),
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="TTL checker actions",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_ttl_checker_actions", by_labels=["type"]
                        )
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="TTL checker compact duration",
                description="The time consumed when executing GC tasks",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_ttl_checker_compact_duration",
            ),
            stat_panel(
                title="TTL checker poll interval",
                format=UNITS.MILLI_SECONDS,
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_ttl_checker_poll_interval",
                            label_selectors=['type="tikv_gc_run_interval"'],
                            by_labels=[],  # override default by instance.
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def PD() -> RowPanel:
    layout = Layout(title="PD")
    layout.row(
        [
            graph_panel(
                title="PD requests",
                description="The count of requests that TiKV sends to PD",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_pd_request_duration_seconds_count",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="PD request duration (average)",
                description="The time consumed by requests that TiKV sends to PD",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_avg(
                            "tikv_pd_request_duration_seconds",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PD heartbeats",
                description="The total number of PD heartbeat messages",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_pd_heartbeat_message_total",
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_pd_pending_heartbeat_total",
                        ),
                        legend_format="{{instance}}-pending",
                    ),
                ],
            ),
            graph_panel(
                title="PD validate peers",
                description="The total number of peers validated by the PD worker",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_pd_validate_peer_total",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PD reconnection",
                description="The count of reconnection between TiKV and PD",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_pd_reconnect_total",
                            range_selector="$__rate_interval",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="PD forward status",
                description="The forward status of PD client",
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_pd_request_forwarded",
                        ),
                        legend_format="{{instance}}-{{host}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Pending TSO Requests",
                description="The number of TSO requests waiting in the queue.",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_pd_pending_tso_request_total",
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def IOBreakdown() -> RowPanel:
    layout = Layout(title="IO Breakdown")
    layout.row(
        [
            graph_panel(
                title="Write IO bytes",
                description="The throughput of disk write per IO type",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_io_bytes",
                            label_selectors=['op="write"'],
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_io_bytes",
                            label_selectors=['op="write"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total",
                    ),
                ],
            ),
            graph_panel(
                title="Read IO bytes",
                description="The throughput of disk read per IO type",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_io_bytes",
                            label_selectors=['op="read"'],
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_io_bytes",
                            label_selectors=['op="read"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="IO threshold",
                description="The threshold of disk IOs per priority",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_rate_limiter_max_bytes_per_sec",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Rate Limiter Request Wait Duration",
                description="IO rate limiter request wait duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_rate_limiter_request_wait_duration_seconds",
                            by_labels=["type"],
                        ),
                        legend_format=r"{{type}}-99%",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_rate_limiter_request_wait_duration_seconds",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftWaterfall() -> RowPanel:
    layout = Layout(title="Raft Waterfall")
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Storage async write duration",
                description="The time consumed by processing asynchronous write requests",
                yaxes=yaxes(left_format=UNITS.SECONDS, right_format=UNITS.NONE_FORMAT),
                metric="tikv_storage_engine_async_request_duration_seconds",
                label_selectors=['type="write"'],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store duration",
                description="The store time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_duration_secs",
            ),
            graph_panel_histogram_quantiles(
                title="Apply duration",
                description="The apply time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_apply_duration_secs",
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store propose wait duration",
                description="The propose wait time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_request_wait_time_duration_secs",
            ),
            graph_panel_histogram_quantiles(
                title="Store batch wait duration",
                description="The batch wait time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_batch_wait_duration_seconds",
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store send to write queue duration",
                description="The send-to-write-queue time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_send_to_queue_duration_seconds",
            ),
            graph_panel_histogram_quantiles(
                title="Store send proposal duration",
                description="The send raft message of the proposal duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_send_proposal_duration_seconds",
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store write kv db end duration",
                description="The write kv db end duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_write_kvdb_end_duration_seconds",
            ),
            graph_panel_histogram_quantiles(
                title="Store before write duration",
                description="The before write time duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_before_write_duration_seconds",
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store persist duration",
                description="The persist duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_persist_duration_seconds",
            ),
            graph_panel_histogram_quantiles(
                title="Store write end duration",
                description="The write end duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_write_end_duration_seconds",
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Store commit but not persist duration",
                description="The commit but not persist duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_commit_not_persist_log_duration_seconds",
            ),
            graph_panel_histogram_quantiles(
                title="Store commit and persist duration",
                description="The commit and persist duration of each request",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_raftstore_store_wf_commit_log_duration_seconds",
            ),
        ]
    )
    return layout.row_panel


def RaftIO() -> RowPanel:
    layout = Layout(title="Raft IO")
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Process ready duration",
            heatmap_description="The time consumed for peer processes to be ready in Raft",
            graph_title="99% Process ready duration per server",
            graph_description="The time consumed for peer processes to be ready in Raft",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_raft_process_duration_secs",
            label_selectors=['type="ready"'],
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Store write loop duration",
            heatmap_description="The time duration of store write loop when store-io-pool-size is not zero.",
            graph_title="99% Store write loop duration per server",
            graph_description="The time duration of store write loop on each TiKV instance when store-io-pool-size is not zero.",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_store_write_loop_duration_seconds",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Append log duration",
            heatmap_description="The time consumed when Raft appends log",
            graph_title="99% Append log duration per server",
            graph_description="The time consumed when Raft appends log on each TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_append_log_duration_seconds",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Commit log duration",
            heatmap_description="The time consumed when Raft commits log",
            graph_title="99% Commit log duration per server",
            graph_description="The time consumed when Raft commits log on each TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_commit_log_duration_seconds",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Apply log duration",
            heatmap_description="The time consumed when Raft applies log",
            graph_title="99% Apply log duration per server",
            graph_description="The time consumed for Raft to apply logs per TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_apply_log_duration_seconds",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Raft Client Wait Connection Ready Duration",
            heatmap_description="The time consumed for Raft Client wait connection ready",
            graph_title="99% Raft Client Wait Connection Ready Duration",
            graph_description="The time consumed for Raft Client wait connection ready per TiKV instance",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_server_raft_client_wait_ready_duration",
            graph_by_labels=["to"],
        )
    )
    layout.row(
        [
            graph_panel(
                title="Store io task reschedule",
                description="The throughput of disk write per IO type",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_io_reschedule_region_total",
                        ),
                        legend_format="rechedule-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_io_reschedule_pending_tasks_total",
                        ),
                        legend_format="pending-task-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="99% Write task block duration per server",
                description="The time consumed when store write task block on each TiKV instance",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_store_write_msg_block_wait_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftPropose() -> RowPanel:
    layout = Layout(title="Raft Propose")
    layout.row(
        [
            graph_panel(
                title="Raft proposals per ready",
                description="The proposal count of a Regions in a tick",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_apply_proposal",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Raft read/write proposals",
                description="The number of proposals per type",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_proposal_total",
                            label_selectors=['type=~"local_read|normal|read_index"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft read proposals per server",
                description="The number of read proposals which are made by each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_proposal_total",
                            label_selectors=['type=~"local_read|read_index"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raft write proposals per server",
                description="The number of write proposals which are made by each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_proposal_total",
                            label_selectors=['type=~"normal"'],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Propose wait duration",
            heatmap_description="The wait time of each proposal",
            graph_title="99% Propose wait duration per server",
            graph_description="The wait time of each proposal in each TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_request_wait_time_duration_secs",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Store write wait duration",
            heatmap_description="The wait time of each store write task",
            graph_title="99% Store write wait duration per server",
            graph_description="The wait time of each store write task in each TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_store_write_task_wait_duration_secs",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Apply wait duration",
            heatmap_description="The wait time of each apply task",
            graph_title="99% Apply wait duration per server",
            graph_description="The wait time of each apply task in each TiKV instance",
            graph_by_labels=["instance"],
            graph_hides=["count", "avg"],
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_apply_wait_time_duration_secs",
        )
    )
    layout.row(
        [
            heatmap_panel(
                title="Store write handle msg duration",
                description="The handle duration of each store write task msg",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_raftstore_store_write_handle_msg_duration_secs_bucket",
            ),
            heatmap_panel(
                title="Store write trigger size",
                description="The distribution of write trigger size",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
                metric="tikv_raftstore_store_write_trigger_wb_bytes_bucket",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft propose speed",
                description="The rate at which peers propose logs",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_propose_log_size_sum",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Perf Context duration",
                description="The rate at which peers propose logs",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_store_perf_context_time_duration_secs",
                            by_labels=["type"],
                        ),
                        legend_format="store-{{type}}",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_apply_perf_context_time_duration_secs",
                            by_labels=["type"],
                        ),
                        legend_format="apply-{{type}}",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftProcess() -> RowPanel:
    layout = Layout(title="Raft Process")
    layout.row(
        [
            graph_panel(
                title="Ready handled",
                description="The count of different ready type of Raft",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_ready_handled_total",
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_process_duration_secs_count",
                            label_selectors=['type="ready"'],
                            by_labels=[],  # overwrite default by instance.
                        ),
                        legend_format="count",
                    ),
                ],
            ),
            graph_panel(
                title="Max duration of raft store events",
                description="The max time consumed by raftstore events",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.999999,
                            "tikv_raftstore_event_duration",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999999,
                            "tikv_broadcast_normal_duration_seconds",
                        ),
                        legend_format="broadcast_normal",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Replica read lock checking duration",
                description="Replica read lock checking duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_replica_read_lock_check_duration_seconds_bucket",
            ),
            heatmap_panel(
                title="Peer msg length distribution",
                description="The length of peer msgs for each round handling",
                metric="tikv_raftstore_peer_msg_len_bucket",
            ),
        ]
    )
    return layout.row_panel


def RaftMessage() -> RowPanel:
    layout = Layout(title="Raft Message")
    layout.row(
        [
            graph_panel(
                title="Sent messages per server",
                description="The number of Raft messages sent by each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_sent_message_total",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Flush messages per server",
                description="The number of Raft messages flushed by each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_server_raft_message_flush_total",
                            by_labels=["instance", "reason"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Receive messages per server",
                description="The number of Raft messages received by each TiKV instance",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_server_raft_message_recv_total",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Messages",
                description="The number of different types of Raft messages that are sent",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_sent_message_total",
                            label_selectors=['status="accept"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Vote",
                description="The total number of vote messages that are sent in Raft",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_sent_message_total",
                            label_selectors=['type="vote"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raft dropped messages",
                description="The number of dropped Raft messages per type",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_dropped_message_total",
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_sent_message_total",
                            label_selectors=['status="drop"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftAdmin() -> RowPanel:
    layout = Layout(title="Raft Admin")
    layout.row(
        [
            graph_panel(
                title="Admin proposals",
                description="The number of admin proposals",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_proposal_total",
                            label_selectors=['type=~"conf_change|transfer_leader"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Admin apply",
                description="The number of the processed apply command",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_admin_cmd_total",
                            label_selectors=['type!="compact"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Check split",
                description="The number of raftstore split checks",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_check_split_total",
                            label_selectors=['type!="ignore"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="99.99% Check split duration",
                description="The time consumed when running split check in .9999",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_raftstore_check_split_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Load base split event",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_load_base_split_event",
                            range_selector="1m",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Load base split duration",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.80,
                            "tikv_load_base_split_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="80%-{{instance}}",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_load_base_split_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="99%-{{instance}}",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_load_base_split_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="avg-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Peer in Flashback State",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_peer_in_flashback_state",
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftLog() -> RowPanel:
    layout = Layout(title="Raft Log")
    layout.row(
        [
            graph_panel(
                title="Raft log GC write duration",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=10),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_raftstore_raft_log_gc_write_duration_secs",
                            by_labels=["instance"],
                        ),
                        legend_format="99.99%-{{instance}}",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_raftstore_raft_log_gc_write_duration_secs",
                            by_labels=["instance"],
                        ),
                        legend_format="avg-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Raft log GC kv sync duration",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=10),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_raftstore_raft_log_kv_sync_duration_secs",
                            by_labels=["instance"],
                        ),
                        legend_format="99.99%-{{instance}}",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_raftstore_raft_log_kv_sync_duration_secs",
                            by_labels=["instance"],
                        ),
                        legend_format="avg-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft log GC write operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_log_gc_write_duration_secs_count",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raft log GC seek operations ",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_log_gc_seek_operations_count",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft log lag",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_log_lag_sum",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raft log gc skipped",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_log_gc_skipped",
                            by_labels=["instance", "reason"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft log GC failed",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_raft_log_gc_failed",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Raft log fetch ",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_entry_fetches",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft log async fetch task duration",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=10),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_raftstore_entry_fetches_task_duration_seconds",
                        ),
                        legend_format="99.99%",
                    ),
                    target(
                        expr=expr_histogram_avg(
                            "tikv_raftstore_entry_fetches_task_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="avg-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_worker_pending_task_total",
                            label_selectors=['name=~"raftlog-fetch-worker"'],
                        ),
                        legend_format="pending-task",
                    ),
                ],
                series_overrides=[
                    series_override(
                        alias="/pending-task/",
                        yaxis=2,
                        transform_negative_y=True,
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def LocalReader() -> RowPanel:
    layout = Layout(title="Local Reader")
    layout.row(
        [
            graph_panel(
                title="Local reader requests",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_local_read_reject_total",
                            by_labels=["instance", "reason"],
                        ),
                        legend_format="{{instance}}-reject-by-{{reason}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_local_read_executed_requests",
                        ),
                        legend_format="{{instance}}-total",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_local_read_executed_stale_read_requests",
                        ),
                        legend_format="{{instance}}-stale-read",
                    ),
                ],
                series_overrides=[
                    series_override(
                        alias="/.*-total/",
                        yaxis=2,
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def UnifiedReadPool() -> RowPanel:
    layout = Layout(title="Unified Read Pool")
    layout.row(
        [
            graph_panel(
                title="Time used by level",
                description="The time used by each level in the unified read pool per second. Level 0 refers to small queries.",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_multilevel_level_elapsed",
                            label_selectors=['name="unified-read-pool"'],
                            by_labels=["level"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Level 0 chance",
                description="The chance that level 0 (small) tasks are scheduled in the unified read pool.",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_multilevel_level0_chance",
                            label_selectors=['name="unified-read-pool"'],
                        ),
                        legend_format="{{type}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Running tasks",
                description="The number of concurrently running tasks in the unified read pool.",
                targets=[
                    target(
                        expr=expr_sum_aggr_over_time(
                            "tikv_unified_read_pool_running_tasks",
                            "avg",
                            "1m",
                            by_labels=["priority"],
                        ),
                        legend_format="{{priority}}",
                    ),
                ],
            ),
            heatmap_panel(
                title="Unified Read Pool Wait Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_yatp_pool_schedule_wait_duration_bucket",
                label_selectors=['name=~"unified-read.*"'],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Duration of One Time Slice",
                description="Unified read pool task execution time during one schedule.",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                metric="tikv_yatp_task_poll_duration",
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Task Execute Duration",
                description="Unified read pool task total execution duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                metric="tikv_yatp_task_exec_duration",
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Task Schedule Times",
                description="Task schedule number of times.",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT, log_base=2),
                metric="tikv_yatp_task_execute_times",
                hide_count=True,
            ),
        ]
    )
    return layout.row_panel


def Storage() -> RowPanel:
    layout = Layout(title="Storage")
    layout.row(
        [
            graph_panel(
                title="Storage command total",
                description="The total count of different kinds of commands received",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC, log_base=10),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_command_total",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Storage async request error",
                description="The total number of engine asynchronous request errors",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_engine_async_request_total",
                            label_selectors=['status!~"all|success"'],
                            by_labels=["status"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Storage async write duration",
            heatmap_description="The time consumed by processing asynchronous write requests",
            graph_title="Storage async write duration",
            graph_description="The storage async write duration",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_storage_engine_async_request_duration_seconds",
            label_selectors=['type="write"'],
        ),
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Storage async snapshot duration",
            heatmap_description="The time consumed by processing asynchronous snapshot requests",
            graph_title="Storage async snapshot duration",
            graph_description="The storage async snapshot duration",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_storage_engine_async_request_duration_seconds",
            label_selectors=['type="snapshot"'],
        ),
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Storage async snapshot duration (pure local read)",
            heatmap_description="The storage async snapshot duration without the involving of raftstore",
            graph_title="Storage async snapshot duration (pure local read)",
            graph_description="The storage async snapshot duration without the involving of raftstore",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_storage_engine_async_request_duration_seconds",
            label_selectors=['type="snapshot_local_read"'],
        ),
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Read index propose wait duration",
            heatmap_description="Read index propose wait duration associated with async snapshot",
            graph_title="Read index propose wait duration",
            graph_description="Read index propose wait duration associated with async snapshot",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_storage_engine_async_request_duration_seconds",
            label_selectors=['type="snapshot_read_index_propose_wait"'],
        ),
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Read index confirm duration",
            heatmap_description="Read index confirm duration associated with async snapshot",
            graph_title="Read index confirm duration",
            graph_description="Read index confirm duration associated with async snapshot",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_storage_engine_async_request_duration_seconds",
            label_selectors=['type="snapshot_read_index_confirm"'],
        ),
    )
    layout.row(
        [
            graph_panel(
                title="Process Stat Cpu Usage",
                description="CPU usage measured over a 30 second window",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_storage_process_stat_cpu_usage",
                        ),
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="Full compaction duration seconds",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_storage_full_compact_duration_seconds",
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Full compaction pause duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_storage_full_compact_pause_duration_seconds",
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Full compaction per-increment duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_storage_full_compact_increment_duration_seconds",
                hide_count=True,
            ),
        ]
    )
    return layout.row_panel


def FlowControl() -> RowPanel:
    layout = Layout(title="Flow Control")
    layout.row(
        [
            graph_panel(
                title="Scheduler flow",
                description="",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_write_flow",
                        ),
                        legend_format="write-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_throttle_flow",
                        ).extra(" != 0"),
                        legend_format="throttle-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler discard ratio",
                description="",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_discard_ratio",
                            by_labels=["type"],
                        ).extra(" / 10000000"),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Throttle duration",
                metric="tikv_scheduler_throttle_duration_seconds_bucket",
                yaxis=yaxis(format=UNITS.SECONDS),
            ),
            graph_panel(
                title="Scheduler throttled CF",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_scheduler_throttle_cf",
                        ).extra(" != 0"),
                        legend_format="{{instance}}-{{cf}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Flow controller actions",
                description="",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_throttle_action_total",
                            by_labels=["type", "cf"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Flush/L0 flow",
                description="",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_l0_flow",
                            by_labels=["instance", "cf"],
                        ),
                        legend_format="{{cf}}_l0_flow-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_flush_flow",
                            by_labels=["instance", "cf"],
                        ),
                        legend_format="{{cf}}_flush_flow-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_l0_flow",
                        ),
                        legend_format="total_l0_flow-{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_flush_flow",
                        ),
                        legend_format="total_flush_flow-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Flow controller factors",
                description="",
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_scheduler_l0",
                        ),
                        legend_format="l0-{{instance}}",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_scheduler_memtable",
                        ),
                        legend_format="memtable-{{instance}}",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_scheduler_l0_avg",
                        ),
                        legend_format="avg_l0-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Compaction pending bytes",
                description="",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_pending_compaction_bytes",
                            label_selectors=['db="kv"'],
                            by_labels=["cf"],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_pending_compaction_bytes",
                            by_labels=["cf"],
                        ).extra(" / 10000000"),
                        legend_format="pending-bytes-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Txn command throttled duration",
                description="Throttle time for txn storage commands in 1 minute.",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_txn_command_throttle_time_total",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Non-txn command throttled duration",
                description="Throttle time for non-txn related processing like analyze or dag in 1 minute.",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_non_txn_command_throttle_time_total",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def SchedulerCommands() -> RowPanel:
    layout = Layout(title="Scheduler", repeat="command")
    layout.row(
        [
            graph_panel(
                title="Scheduler stage total",
                description="The total number of commands on each stage in commit command",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_too_busy_total",
                            label_selectors=['type="$command"'],
                        ),
                        legend_format="busy-{{instance}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_stage_total",
                            label_selectors=['type="$command"'],
                            by_labels=["stage"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Scheduler command duration",
                description="The time consumed when executing commit command",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_scheduler_command_duration_seconds",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Scheduler latch wait duration",
                description="The time which is caused by latch wait in commit command",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_scheduler_latch_wait_duration_seconds",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Scheduler keys read",
                description="The count of keys read by a commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                metric="tikv_scheduler_kv_command_key_read",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Scheduler keys written",
                description="The count of keys written by a commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                metric="tikv_scheduler_kv_command_key_write",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Scheduler scan details",
                description="The keys scan details of each CF when executing commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_kv_scan_details",
                            label_selectors=['req="$command"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler scan details [lock]",
                description="The keys scan details of lock CF when executing commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_kv_scan_details",
                            label_selectors=['req="$command", cf="lock"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Scheduler scan details [write]",
                description="The keys scan details of write CF when executing commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_kv_scan_details",
                            label_selectors=['req="$command", cf="write"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler scan details [default]",
                description="The keys scan details of default CF when executing commit command",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_kv_scan_details",
                            label_selectors=['req="$command", cf="default"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Scheduler command read duration",
                description="The time consumed on reading when executing commit command",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_scheduler_processing_read_duration_seconds",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
            heatmap_panel(
                title="Check memory locks duration",
                description="The time consumed on checking memory locks",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_storage_check_mem_lock_duration_seconds_bucket",
                label_selectors=['type="$command"'],
            ),
        ]
    )
    return layout.row_panel


def Scheduler() -> RowPanel:
    layout = Layout(title="Scheduler")
    layout.row(
        [
            graph_panel(
                title="Scheduler stage total",
                description="The total number of commands on each stage",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_too_busy_total",
                            by_labels=["stage"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_stage_total",
                            by_labels=["stage"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler priority commands",
                description="The count of different priority commands",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_scheduler_commands_pri_total",
                            by_labels=["priority"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Scheduler pending commands",
                description="The count of pending commands per TiKV instance",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_contex_total",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler running commands",
                description="The count of running commands per TiKV instance",
                yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_running_commands",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Scheduler writing bytes",
                description="The total writing bytes of commands on each stage",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_writing_bytes",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Scheduler memory quota",
                description="The number of bytes used by scheduler",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_memory_quota_size",
                            label_selectors=['type="in_use"'],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_scheduler_memory_quota_size",
                            label_selectors=['type="capacity"'],
                        ),
                        hide=True,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Txn Scheduler Pool Wait Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_yatp_pool_schedule_wait_duration_bucket",
                label_selectors=['name=~"sched-worker.*"'],
            ),
        ]
    )
    return layout.row_panel


def GC() -> RowPanel:
    layout = Layout(title="GC")
    layout.row(
        [
            graph_panel(
                title="GC tasks",
                description="The count of GC tasks processed by gc_worker",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_gcworker_gc_tasks_vec",
                            by_labels=["task"],
                        ),
                        legend_format="total-{{task}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_gc_skipped_counter",
                            by_labels=["task"],
                        ),
                        legend_format="skipped-{{task}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gcworker_gc_task_fail_vec",
                            by_labels=["task"],
                        ),
                        legend_format="failed-{{task}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_worker_too_busy",
                            by_labels=[],
                        ),
                        legend_format="gcworker-too-busy",
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="GC tasks duration",
                description="The time consumed when executing GC tasks",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_gcworker_gc_task_duration_vec",
                label_selectors=['type="$command"'],
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="TiDB GC seconds",
                description="The GC duration",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            1, "tidb_tikvclient_gc_seconds", by_labels=["instance"]
                        ).skip_default_instance_selector(),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="TiDB GC worker actions",
                description="The count of TiDB GC worker actions",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tidb_tikvclient_gc_worker_actions_total",
                            by_labels=["type"],
                        ).skip_default_instance_selector(),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="ResolveLocks Progress",
                description="Progress of ResolveLocks, the first phase of GC",
                targets=[
                    target(
                        expr=expr_max(
                            "tidb_tikvclient_range_task_stats",
                            label_selectors=['type=~"resolve-locks.*"'],
                            by_labels=["result"],
                        ).skip_default_instance_selector(),
                    ),
                ],
            ),
            graph_panel(
                title="TiKV Auto GC Progress",
                description="Progress of TiKV's GC",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum(
                                "tikv_gcworker_autogc_processed_regions",
                                label_selectors=['type="scan"'],
                            ),
                            "/",
                            expr_sum(
                                "tikv_raftstore_region_count",
                                label_selectors=['type="region"'],
                            ),
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="GC speed",
                description="keys / second",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_storage_mvcc_gc_delete_versions_sum",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_keys/s",
                    ),
                ],
            ),
            graph_panel(
                title="TiKV Auto GC SafePoint",
                description="SafePoint used for TiKV's Auto GC",
                yaxes=yaxes(left_format=UNITS.DATE_TIME_ISO),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_gcworker_autogc_safe_point",
                        )
                        .extra("/ (2^18)")
                        .skip_default_instance_selector(),
                    ),
                ],
            ),
        ]
    )
    layout.half_row(
        [
            stat_panel(
                title="GC lifetime",
                description="The lifetime of TiDB GC",
                format=UNITS.SECONDS,
                targets=[
                    target(
                        expr=expr_max(
                            "tidb_tikvclient_gc_config",
                            label_selectors=['type="tikv_gc_life_time"'],
                            by_labels=[],
                        ).skip_default_instance_selector(),
                    ),
                ],
            ),
            stat_panel(
                title="GC interval",
                description="The interval of TiDB GC",
                format=UNITS.SECONDS,
                targets=[
                    target(
                        expr=expr_max(
                            "tidb_tikvclient_gc_config",
                            label_selectors=['type="tikv_gc_run_interval"'],
                            by_labels=[],
                        ).skip_default_instance_selector(),
                    ),
                ],
            ),
        ]
    )
    layout.half_row(
        [
            graph_panel(
                title="GC in Compaction Filter",
                description="Keys handled in GC compaction filter",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filtered",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_filtered",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_skip",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_skipped",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_mvcc_rollback",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_mvcc-rollback/mvcc-lock",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_orphan_versions",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_orphan-versions",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_perform",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_performed-times",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_failure",
                            by_labels=["key_mode", "type"],
                        ),
                        legend_format="{{key_mode}}_failure-{{type}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_mvcc_deletion_met",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_mvcc-deletion-met",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_mvcc_deletion_handled",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_mvcc-deletion-handled",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_gc_compaction_filter_mvcc_deletion_wasted",
                            by_labels=["key_mode"],
                        ),
                        legend_format="{{key_mode}}_mvcc-deletion-wasted",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="GC scan write details",
                description="GC scan write details",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_gcworker_gc_keys",
                            label_selectors=['cf="write"'],
                            by_labels=["key_mode", "tag"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="GC scan default details",
                description="GC scan default details",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_gcworker_gc_keys",
                            label_selectors=['cf="default"'],
                            by_labels=["key_mode", "tag"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Snapshot() -> RowPanel:
    layout = Layout(title="Snapshot")
    layout.row(
        [
            graph_panel(
                title="Rate snapshot message",
                description="The rate of Raft snapshot messages sent",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_raftstore_raft_sent_message_total",
                            range_selector="1m",
                            label_selectors=['type="snapshot"'],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Snapshot state count",
                description="The number of snapshots in different states",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_snapshot_traffic_total",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="99% Snapshot generation wait duration",
                description="The time snapshot generation tasks waited to be scheduled. ",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_snapshot_generation_wait_duration_seconds",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="99% Handle snapshot duration",
                description="The time consumed when handling snapshots",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_send_snapshot_duration_seconds",
                        ),
                        legend_format="send",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_snapshot_duration_seconds",
                            label_selectors=['type="apply"'],
                        ),
                        legend_format="apply",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_snapshot_duration_seconds",
                            label_selectors=['type="generate"'],
                        ),
                        legend_format="generate",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="99.99% Snapshot size",
                description="The snapshot size (P99.99).9999",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_snapshot_size",
                        ),
                        legend_format="size",
                    ),
                ],
            ),
            graph_panel(
                title="99.99% Snapshot KV count",
                description="The number of KV within a snapshot in .9999",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9999,
                            "tikv_snapshot_kv_count",
                        ),
                        legend_format="count",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Snapshot Actions",
                description="Action stats for snapshot generating and applying",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_raftstore_snapshot_total",
                            range_selector="1m",
                            by_labels=["type", "status"],
                        ),
                    ),
                    target(
                        expr=expr_sum_delta(
                            "tikv_raftstore_clean_region_count",
                            range_selector="1m",
                            by_labels=["type", "status"],
                        ),
                        legend_format="clean-region-by-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Snapshot transport speed",
                description="The speed of sending or receiving snapshot",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_snapshot_limit_transport_bytes",
                            by_labels=["instance", "type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_snapshot_limit_generate_bytes",
                        ),
                        legend_format="{{instance}}-generate",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Task() -> RowPanel:
    layout = Layout(title="Task")
    layout.row(
        [
            graph_panel(
                title="Worker handled tasks",
                description="The number of tasks handled by worker",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_worker_handled_task_total",
                            by_labels=["name"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Worker pending tasks",
                description="Current pending and running tasks of worker",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_worker_pending_task_total",
                            by_labels=["name"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="FuturePool handled tasks",
                description="The number of tasks handled by future_pool",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_futurepool_handled_task_total",
                            by_labels=["name"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="FuturePool pending tasks",
                description="Current pending and running tasks of future_pool",
                targets=[
                    target(
                        expr=expr_sum_aggr_over_time(
                            "tikv_futurepool_pending_task_total",
                            "avg",
                            range_selector="1m",
                            by_labels=["name"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def CoprocessorOverview() -> RowPanel:
    layout = Layout(title="Coprocessor Overview")
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Request duration",
            heatmap_description="The time consumed to handle coprocessor read requests",
            graph_title="Request duration",
            graph_description="The time consumed to handle coprocessor read requests",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_coprocessor_request_duration_seconds",
            graph_by_labels=["req"],
        ),
    )
    layout.row(
        [
            graph_panel(
                title="Total Requests",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_request_duration_seconds_count",
                            by_labels=["req"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Total Request Errors",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_request_error",
                            by_labels=["reason"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="KV Cursor Operations",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_scan_keys_sum",
                            by_labels=["req"],
                        ),
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="KV Cursor Operations",
                description="",
                metric="tikv_coprocessor_scan_keys",
                yaxes=yaxes(left_format=UNITS.SHORT),
                by_labels=["req"],
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Total RocksDB Perf Statistics",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            label_selectors=['metric="internal_delete_skipped_count"'],
                            by_labels=["req"],
                        ),
                        legend_format="delete_skipped-{{req}}",
                    ),
                ],
            ),
            graph_panel(
                title="Total Response Size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_response_bytes",
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def CoprocessorDetail() -> RowPanel:
    layout = Layout(title="Coprocessor Detail")
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Handle duration",
                description="The time consumed when handling coprocessor requests",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_coprocessor_request_handle_seconds",
                by_labels=["req"],
                hide_avg=True,
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Handle duration by store",
                description="The time consumed to handle coprocessor requests per TiKV instance",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_coprocessor_request_handle_seconds",
                by_labels=["req", "instance"],
                hide_avg=True,
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Wait duration",
                description="The time consumed when coprocessor requests are wait for being handled",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_coprocessor_request_wait_seconds",
                label_selectors=['type="all"'],
                by_labels=["req"],
                hide_avg=True,
                hide_count=True,
            ),
            graph_panel_histogram_quantiles(
                title="Wait duration by store",
                description="The time consumed when coprocessor requests are wait for being handled in each TiKV instance",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_coprocessor_request_wait_seconds",
                label_selectors=['type="all"'],
                by_labels=["req", "instance"],
                hide_avg=True,
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Total DAG Requests",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_dag_request_count",
                            by_labels=["vec_type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Total DAG Executors",
                description="The total number of DAG executors",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_executor_count",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Total Ops Details (Table Scan)",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_scan_details",
                            label_selectors=['req="select"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Total Ops Details (Index Scan)",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_scan_details",
                            label_selectors=['req="index"'],
                            by_labels=["tag"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Total Ops Details by CF (Table Scan)",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_scan_details",
                            label_selectors=['req="select"'],
                            by_labels=["cf", "tag"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Total Ops Details by CF (Index Scan)",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_scan_details",
                            label_selectors=['req="index"'],
                            by_labels=["cf", "tag"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Memory lock checking duration",
            heatmap_description="The time consumed on checking memory locks for coprocessor requests",
            graph_title="Memory lock checking duration",
            graph_description="The time consumed on checking memory locks for coprocessor requests",
            yaxis_format=UNITS.SECONDS,
            metric="tikv_coprocessor_mem_lock_check_duration_seconds",
        ),
    )
    return layout.row_panel


def RangeCacheMemoryEngine() -> RowPanel:
    layout = Layout(title="Range Cache Memory Engine")
    layout.row(
        [
            graph_panel(
                title="Snapshot Type Count",
                description="Count of each snapshot type",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_snapshot_type_count",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Snapshot Failed Reason",
                description="Reasons for why rance cache snapshot is not acquired",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_range_cache_snapshot_acquire_failed_reason_count",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="GC Filter",
                description="Rang cache engine garbage collection information",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_range_cache_memory_engine_gc_filtered",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Threads() -> RowPanel:
    layout = Layout(title="Threads")
    layout.row(
        [
            graph_panel(
                title="Threads state",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_threads_state",
                            by_labels=["instance", "state"],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_threads_state",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}-total",
                    ),
                ],
            ),
            graph_panel(
                title="Threads IO",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_topk(
                            20,
                            "%s"
                            % expr_sum_rate(
                                "tikv_threads_io_bytes_total",
                                by_labels=["name", "io"],
                            ).extra("> 1024"),
                        ),
                        legend_format="{{name}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Thread Voluntary Context Switches",
                targets=[
                    target(
                        expr=expr_topk(
                            20,
                            "%s"
                            % expr_max_rate(
                                "tikv_thread_voluntary_context_switches",
                                by_labels=["name"],
                            ).extra("> 100"),
                        ),
                        legend_format="{{name}}",
                    ),
                ],
            ),
            graph_panel(
                title="Thread Nonvoluntary Context Switches",
                targets=[
                    target(
                        expr=expr_topk(
                            20,
                            "%s"
                            % expr_max_rate(
                                "tikv_thread_nonvoluntary_context_switches",
                                by_labels=["name"],
                            ).extra("> 100"),
                        ),
                        legend_format="{{name}}",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RocksDB() -> RowPanel:
    layout = Layout(title="RocksDB", repeat="db")
    layout.row(
        [
            graph_panel(
                title="Get operations",
                description="The count of get operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_memtable_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="memtable_hit"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="memtable",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type=~"block_cache_data_hit|block_cache_filter_hit"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="block_cache",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_get_served",
                            label_selectors=[
                                'db="$db"',
                                'type="get_hit_l0"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="l0",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_get_served",
                            label_selectors=[
                                'db="$db"',
                                'type="get_hit_l1"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="l1",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_get_served",
                            label_selectors=[
                                'db="$db"',
                                'type="get_hit_l2_and_up"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="l2_and_up",
                    ),
                ],
            ),
            graph_panel(
                title="Get duration",
                description="The time consumed when executing get operations",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_get_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="get_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_get_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="get_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_get_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="get_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_get_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="get_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Seek operations",
                description="The count of seek operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_seek"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="seek",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_seek_found"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="seek_found",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_next"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="next",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_next_found"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="next_found",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_prev"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="prev",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_locate",
                            label_selectors=[
                                'db="$db"',
                                'type="number_db_prev_found"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="prev_found",
                    ),
                ],
            ),
            graph_panel(
                title="Seek duration",
                description="The time consumed when executing seek operation",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_seek_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="seek_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_seek_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="seek_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_seek_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="seek_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_seek_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="seek_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write operations",
                description="The count of write operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_write_served",
                            label_selectors=[
                                'db="$db"',
                                'type=~"write_done_by_self|write_done_by_other"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="done",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_write_served",
                            label_selectors=[
                                'db="$db"',
                                'type="write_timeout"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="timeout",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_write_served",
                            label_selectors=[
                                'db="$db"',
                                'type="write_with_wal"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="with_wal",
                    ),
                ],
            ),
            graph_panel(
                title="Write duration",
                description="The time consumed when executing write operation",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_write_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="WAL sync operations",
                description="The count of WAL sync operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_wal_file_synced",
                            label_selectors=[
                                'db="$db"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="sync",
                    ),
                ],
            ),
            graph_panel(
                title="Write WAL duration",
                description="The time consumed when executing write wal operation",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_write_wal_time_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_wal_micros_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_wal_time_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_wal_micros_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_wal_time_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_wal_micros_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_wal_time_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="write_wal_micros_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Compaction operations",
                description="The count of compaction and flush operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_event_total",
                            label_selectors=[
                                'db="$db"',
                            ],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="WAL sync duration",
                description="The time consumed when executing WAL sync operation",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=10),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_wal_file_sync_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="wal_file_sync_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_wal_file_sync_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="wal_file_sync_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_wal_file_sync_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="wal_file_sync_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_wal_file_sync_micro_seconds",
                            label_selectors=[
                                'db="$db"',
                                'type="wal_file_sync_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Compaction guard actions",
                description="Compaction guard actions",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_compaction_guard_action_total",
                            label_selectors=[
                                'cf=~"default|write"',
                            ],
                            by_labels=["cf", " type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Compaction duration",
                description="The time consumed when executing the compaction and flush operations",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_compaction_time",
                            label_selectors=[
                                'db="$db"',
                                'type="compaction_time_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_compaction_time",
                            label_selectors=[
                                'db="$db"',
                                'type="compaction_time_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_compaction_time",
                            label_selectors=[
                                'db="$db"',
                                'type="compaction_time_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_compaction_time",
                            label_selectors=[
                                'db="$db"',
                                'type="compaction_time_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="SST read duration",
                description="The time consumed when reading SST files",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_sst_read_micros",
                            label_selectors=[
                                'db="$db"',
                                'type="sst_read_micros_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_sst_read_micros",
                            label_selectors=[
                                'db="$db"',
                                'type="sst_read_micros_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_sst_read_micros",
                            label_selectors=[
                                'db="$db"',
                                'type="sst_read_micros_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_sst_read_micros",
                            label_selectors=[
                                'db="$db"',
                                'type="sst_read_micros_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
            graph_panel(
                title="Compaction reason",
                description=None,
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_compaction_reason",
                            label_selectors=[
                                'db="$db"',
                            ],
                            by_labels=["cf", "reason"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Block cache size",
                description="The block cache size. Broken down by column family if shared block cache is disabled.",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_topk(
                            20,
                            "%s"
                            % expr_avg(
                                "tikv_engine_block_cache_size_bytes",
                                label_selectors=[
                                    'db="$db"',
                                ],
                                by_labels=["cf", "instance"],
                            ),
                        ),
                        legend_format="{{instance}}-{{cf}}",
                    ),
                ],
            ),
            graph_panel(
                title="Memtable hit",
                description="The hit rate of memtable",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_memtable_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="memtable_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_memtable_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="memtable_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_memtable_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="memtable_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="hit",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Block cache flow",
                description="The flow of different kinds of block cache operations",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC, log_base=10),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_byte_read"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total_read",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_byte_write"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total_written",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_data_bytes_insert"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="data_insert",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_filter_bytes_insert"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="filter_insert",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_filter_bytes_evict"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="filter_evict",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_index_bytes_insert"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="index_insert",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_index_bytes_evict"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="index_evict",
                    ),
                ],
            ),
            graph_panel(
                title="Block cache hit",
                description="The hit rate of block cache",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_cache_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="block_cache_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="all",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_cache_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="block_cache_data_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_data_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_data_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="data",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_cache_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="block_cache_filter_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_filter_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_filter_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="filter",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_cache_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="block_cache_index_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_index_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_cache_efficiency",
                                    label_selectors=[
                                        'db="$db"',
                                        'type="block_cache_index_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="index",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_bloom_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="bloom_prefix_useful"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_sum_rate(
                                "tikv_engine_bloom_efficiency",
                                label_selectors=[
                                    'db="$db"',
                                    'type="bloom_prefix_checked"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                        ),
                        legend_format="bloom prefix",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Keys flow",
                description="The flow of different kinds of operations on keys",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="keys_read"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="read",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="keys_written"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="written",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_compaction_num_corrupt_keys",
                            label_selectors=[
                                'db="$db"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="corrupt",
                    ),
                ],
            ),
            graph_panel(
                title="Block cache operations",
                description="The count of different kinds of block cache operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_add"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total_add",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_data_add"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="data_add",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_filter_add"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="filter_add",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_index_add"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="index_add",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_cache_efficiency",
                            label_selectors=[
                                'db="$db"',
                                'type="block_cache_add_failures"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="add_failures",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Read flow",
                description="The flow rate of read operations per type",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_read"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="get",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="iter_bytes_read"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="scan",
                    ),
                ],
            ),
            graph_panel(
                title="Total keys",
                description="The count of keys in each column family",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_estimate_num_keys",
                            label_selectors=[
                                'db="$db"',
                            ],
                            by_labels=["cf"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write flow",
                description="The flow of different kinds of write operations",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="wal_file_bytes"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="wal",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_written"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="write",
                    ),
                ],
            ),
            graph_panel(
                title="Bytes / Read",
                description="The bytes per read",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC, log_base=10),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_bytes_per_read",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_read_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_read",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_read_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_read",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_read_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_read",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_read_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Compaction flow",
                description="The flow rate of compaction operations per type",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_compaction_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_read"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="read",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_compaction_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_written"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="written",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            label_selectors=[
                                'db="$db"',
                                'type="flush_write_bytes"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="flushed",
                    ),
                ],
            ),
            graph_panel(
                title="Bytes / Write",
                description="The bytes per write",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_bytes_per_write",
                            label_selectors=['db="$db"', 'type="bytes_per_write_max"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_write",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_write_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_write",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_write_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_bytes_per_write",
                            label_selectors=[
                                'db="$db"',
                                'type="bytes_per_write_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Read amplification",
                description="The read amplification per TiKV instance",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_read_amp_flow_bytes",
                                label_selectors=[
                                    'db="$db"',
                                    'type="read_amp_total_read_bytes"',
                                ],
                            ),
                            "/",
                            expr_sum_rate(
                                "tikv_engine_read_amp_flow_bytes",
                                label_selectors=[
                                    'db="$db"',
                                    'type="read_amp_estimate_useful_bytes"',
                                ],
                            ),
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Compaction pending bytes",
                description="The pending bytes to be compacted",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_pending_compaction_bytes",
                            label_selectors=['db="$db"'],
                            by_labels=["cf"],
                        ),
                        legend_format="{{cf}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Number of snapshots",
                description="The number of snapshot of each TiKV instance",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_engine_num_snapshots",
                            label_selectors=['db="$db"'],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Compression ratio",
                description="The compression ratio of each level",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_compression_ratio",
                            label_selectors=['db="$db"'],
                            by_labels=["cf", "level"],
                        ),
                        legend_format="{{cf}}-L{{level}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Number files at each level",
                description="The number of SST files for different column families in each level",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_num_files_at_level",
                            label_selectors=['db="$db"'],
                            by_labels=["cf", "level"],
                        ),
                        legend_format="{{cf}}-L{{level}}",
                    ),
                ],
            ),
            graph_panel(
                title="Oldest snapshots duration",
                description="The time that the oldest unreleased snapshot survivals",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_engine_oldest_snapshot_duration",
                            label_selectors=['db="$db"'],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Stall conditions changed of each CF",
                description="Stall conditions changed of each column family",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_engine_stall_conditions_changed",
                            label_selectors=['db="$db"'],
                        ),
                        legend_format="{{instance}}-{{cf}}-{{type}}",
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="Ingest SST duration seconds",
                description="The time consumed when ingesting SST files",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_snapshot_ingest_sst_duration_seconds",
                label_selectors=['db="$db"'],
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write Stall Reason",
                description=None,
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_write_stall_reason",
                            label_selectors=['db="$db"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Write stall duration",
                description="The time which is caused by write stall",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_max(
                            "tikv_engine_write_stall",
                            label_selectors=['db="$db"', 'type="write_stall_max"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_stall",
                            label_selectors=[
                                'db="$db"',
                                'type="write_stall_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_stall",
                            label_selectors=[
                                'db="$db"',
                                'type="write_stall_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_stall",
                            label_selectors=['db="$db"', 'type="write_stall_average"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Ingestion picked level",
                description="The level that the external file ingests into",
                yaxis=yaxis(format=UNITS.SHORT),
                metric="tikv_engine_ingestion_picked_level_bucket",
                label_selectors=['db="$db"'],
            ),
            graph_panel(
                title="Memtable size",
                description="The memtable size of each column family",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_memory_bytes",
                            label_selectors=['db="$db"', 'type="mem-tables-all"'],
                            by_labels=["cf"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RaftEngine() -> RowPanel:
    layout = Layout(title="Raft Engine")
    layout.row(
        [
            graph_panel(
                title="Operation",
                description="The count of operations per second",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "raft_engine_write_apply_duration_seconds_count",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="write",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "raft_engine_read_entry_duration_seconds_count",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="read_entry",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "raft_engine_read_message_duration_seconds_count",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="read_message",
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="Write Duration",
                description="The time used in write operation",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="raft_engine_write_duration_seconds",
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Flow",
                description="The I/O flow rate",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "raft_engine_write_size_sum",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="write",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "raft_engine_background_rewrite_bytes_sum",
                            by_labels=["type"],
                        ),
                        legend_format="rewrite-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Write Duration Breakdown (99%)",
                description="99% duration breakdown of write operation",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "raft_engine_write_preprocess_duration_seconds"
                        ),
                        legend_format="wait",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "raft_engine_write_leader_duration_seconds"
                        ),
                        legend_format="wal",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "raft_engine_write_apply_duration_seconds"
                        ),
                        legend_format="apply",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Bytes / Written",
                description="The bytes per write",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                metric="raft_engine_write_size",
                hide_count=True,
            ),
            graph_panel(
                title="WAL Duration Breakdown (999%)",
                description="999% duration breakdown of WAL write operation",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_write_leader_duration_seconds"
                        ),
                        legend_format="total",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_sync_log_duration_seconds"
                        ),
                        legend_format="sync",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_allocate_log_duration_seconds"
                        ),
                        legend_format="allocate",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_rotate_log_duration_seconds"
                        ),
                        legend_format="rotate",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="File Count",
                description="The average number of files",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_avg(
                            "raft_engine_log_file_count",
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_avg(
                            "raft_engine_swap_file_count",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="swap",
                    ),
                    target(
                        expr=expr_avg(
                            "raft_engine_recycled_file_count",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}-recycle",
                    ),
                ],
            ),
            graph_panel(
                title="Other Durations (99%)",
                description="The 99% duration of operations other than write",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_read_entry_duration_seconds"
                        ),
                        legend_format="read_entry",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_read_message_duration_seconds"
                        ),
                        legend_format="read_message",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.999, "raft_engine_purge_duration_seconds"
                        ),
                        legend_format="purge",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Entry Count",
                description="The average number of log entries",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_avg(
                            "raft_engine_log_entry_count",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Titan() -> RowPanel:
    layout = Layout(title="Titan", repeat="titan_db")
    layout.row(
        [
            graph_panel(
                title="Blob file count",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_titandb_num_live_blob_file",
                            label_selectors=['db="$titan_db"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="live blob file num",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_engine_titandb_num_obsolete_blob_file",
                            label_selectors=['db="$titan_db"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="obsolete blob file num",
                    ),
                ],
            ),
            graph_panel(
                title="Blob file size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_titandb_live_blob_file_size",
                            label_selectors=['db="$titan_db"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="live blob file size",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_engine_titandb_obsolete_blob_file_size",
                            label_selectors=['db="$titan_db"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="obsolete blob file size",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob cache size",
                description="The blob cache size.",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_topk(
                            20,
                            "%s"
                            % expr_avg(
                                "tikv_engine_blob_cache_size_bytes",
                                label_selectors=['db="$titan_db"'],
                                by_labels=["cf", "instance"],
                            ),
                        ),
                        legend_format="{{instance}}-{{cf}}",
                    ),
                ],
            ),
            graph_panel(
                title="Blob cache hit",
                description="The hit rate of block cache",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_engine_blob_cache_efficiency",
                                label_selectors=[
                                    'db="$titan_db"',
                                    'type="blob_cache_hit"',
                                ],
                                by_labels=[],  # override default by instance.
                            ),
                            "/",
                            expr_operator(
                                expr_sum_rate(
                                    "tikv_engine_blob_cache_efficiency",
                                    label_selectors=[
                                        'db="$titan_db"',
                                        'type="blob_cache_hit"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                                "+",
                                expr_sum_rate(
                                    "tikv_engine_blob_cache_efficiency",
                                    label_selectors=[
                                        'db="$titan_db"',
                                        'type="blob_cache_miss"',
                                    ],
                                    by_labels=[],  # override default by instance.
                                ),
                            ),
                        ),
                        legend_format="all",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Iter touched blob file count",
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_iter_touch_blob_file_count",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_iter_touch_blob_file_count_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_iter_touch_blob_file_count",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_iter_touch_blob_file_count_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_iter_touch_blob_file_count",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_iter_touch_blob_file_count_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_iter_touch_blob_file_count",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_iter_touch_blob_file_count_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob key size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_key_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_key_size_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_key_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_key_size_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_key_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_key_size_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_key_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_key_size_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob value size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_value_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_value_size_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_value_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_value_size_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_value_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_value_size_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_value_size",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_value_size_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob get operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_locate",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="number_blob_get"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="get",
                    ),
                ],
            ),
            graph_panel(
                title="Blob get duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_get_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_average"'],
                            by_labels=["type"],
                        ),
                        legend_format="avg-{{type}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_get_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%-{{type}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_get_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%-{{type}}",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_get_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_max"'],
                            by_labels=["type"],
                        ),
                        legend_format="max-{{type}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob file discardable ratio distribution",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_engine_titandb_blob_file_discardable_ratio",
                            label_selectors=['db="$titan_db"'],
                            by_labels=["ratio"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Blob iter operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_locate",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="number_blob_seek"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="seek",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_locate",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="number_blob_prev"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="prev",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_locate",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="number_blob_next"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="next",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob seek duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_seek_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_average"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_seek_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_seek_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_seek_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_max"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob next duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_next_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_average"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_next_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_next_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_next_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_max"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob prev duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_prev_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_average"'],
                            by_labels=["type"],
                        ),
                        legend_format="avg-{{type}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_prev_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%-{{type}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_prev_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type=~".*_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%-{{type}}",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_prev_micros_seconds",
                            label_selectors=['db="$titan_db"', 'type=~".*_max"'],
                            by_labels=["type"],
                        ),
                        legend_format="max-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Blob keys flow",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_flow_bytes",
                            label_selectors=['db="$titan_db"', 'type=~"keys.*"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob file read duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_read_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_read_micros_average"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_read_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_read_micros_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_read_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_read_micros_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_file_read_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_read_micros_max"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob bytes flow",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_flow_bytes",
                            label_selectors=['db="$titan_db"', 'type=~"bytes.*"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob file write duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_write_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_write_micros_average"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_write_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_write_micros_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_write_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_write_micros_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_file_write_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_write_micros_max"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob file sync operations",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_file_synced",
                            label_selectors=['db="$titan_db"'],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="sync",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob GC action",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_gc_action_count",
                            label_selectors=['db="$titan_db"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Blob file sync duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_sync_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_sync_micros_average"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_sync_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_sync_micros_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_file_sync_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_sync_micros_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_file_sync_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_file_sync_micros_max"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="max",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob GC duration",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_micros_average"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_micros_percentile95"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_micros_percentile99"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_gc_micros_seconds",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_micros_max"',
                            ],
                            by_labels=["type"],
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob GC keys flow",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_gc_flow_bytes",
                            label_selectors=['db="$titan_db"', 'type=~"keys.*"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob GC input file size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_input_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_input_file_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_input_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_input_file_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_input_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_input_file_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_gc_input_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_input_file_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob GC bytes flow",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_gc_flow_bytes",
                            label_selectors=['db="$titan_db"', 'type=~"bytes.*"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Blob GC output file size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_output_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_output_file_average"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="avg",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_output_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_output_file_percentile95"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_blob_gc_output_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_output_file_percentile99"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_max(
                            "tikv_engine_blob_gc_output_file",
                            label_selectors=[
                                'db="$titan_db"',
                                'type="blob_gc_output_file_max"',
                            ],
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="max",
                    ),
                ],
            ),
            graph_panel(
                title="Blob GC file count",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_blob_gc_file_count",
                            label_selectors=['db="$titan_db"'],
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def PessimisticLocking() -> RowPanel:
    layout = Layout(title="Pessimistic Locking")
    layout.row(
        [
            graph_panel(
                title="Lock Manager Thread CPU",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"waiter_manager.*"'],
                            by_labels=["instance", "name"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"deadlock_detect.*"'],
                            by_labels=["instance", "name"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Lock Manager Handled tasks",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_lock_manager_task_counter",
                            by_labels=["type"],
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Waiter lifetime duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                metric="tikv_lock_manager_waiter_lifetime_duration",
                hide_count=True,
            ),
            graph_panel(
                title="Lock Waiting Queue",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_sum_aggr_over_time(
                            "tikv_lock_manager_wait_table_status",
                            "max",
                            "30s",
                            by_labels=["type"],
                        ),
                    ),
                    target(
                        expr=expr_sum_aggr_over_time(
                            "tikv_lock_wait_queue_entries_gauge_vec",
                            "max",
                            "30s",
                            by_labels=["type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Deadlock detect duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                metric="tikv_lock_manager_detect_duration",
                hide_count=True,
            ),
            graph_panel(
                title="Detect error",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_lock_manager_error_counter", by_labels=["type"]
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Deadlock detector leader",
                targets=[
                    target(
                        expr=expr_sum_aggr_over_time(
                            "tikv_lock_manager_detector_leader_heartbeat",
                            "max",
                            "30s",
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Total pessimistic locks memory size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_simple("tikv_pessimistic_lock_memory_size"),
                        legend_format="{{instance}}",
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="In-memory pessimistic locking result",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_in_memory_pessimistic_locking", by_labels=["result"]
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Pessimistic lock activities",
                description="The number of active keys and waiters.",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_lock_wait_queue_entries_gauge_vec", by_labels=["type"]
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Lengths of lock wait queues when transaction enqueues",
                description="The length includes the entering transaction itself",
                yaxis=yaxis(format=UNITS.SHORT),
                metric="tikv_lock_wait_queue_length_bucket",
            ),
            graph_panel_histogram_quantiles(
                title="In-memory scan lock read duration",
                description="The duration scan in-memory pessimistic locks with read lock",
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=2),
                metric="tikv_storage_mvcc_scan_lock_read_duration_seconds",
                by_labels=["type"],
                hide_count=True,
                hide_avg=True,
            ),
        ]
    )
    return layout.row_panel


def PointInTimeRestore() -> RowPanel:
    layout = Layout(title="Point In Time Restore")
    layout.row(
        [
            graph_panel(
                title="CPU Usage",
                description=None,
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"sst_.*"',
                            ],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="P99 RPC Duration",
                description=None,
                yaxes=yaxes(left_format=UNITS.SECONDS, log_base=1),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_import_rpc_duration",
                            label_selectors=[
                                'request="apply"',
                            ],
                        ),
                        legend_format="total-99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_import_apply_duration",
                            label_selectors=[
                                'type=~"queue|exec_download"',
                            ],
                            by_labels=["le", "type"],
                        ),
                        legend_format="(DL){{type}}-99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_import_engine_request",
                            by_labels=["le", "type"],
                        ),
                        legend_format="(AP){{type}}-99",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Import RPC Ops",
                description="",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_rpc_duration_count",
                            label_selectors=[
                                'request="apply"',
                            ],
                            by_labels=["instance", "request"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_rpc_duration_count",
                            label_selectors=[
                                'request!="switch_mode"',
                            ],
                            by_labels=["request"],
                        ),
                        legend_format="total-{{request}}",
                    ),
                ],
            ),
            graph_panel(
                title="Import RPC Count",
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_import_rpc_count",
                            label_selectors=[
                                'type="apply"',
                            ],
                        ),
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Cache Events",
                description=None,
                yaxes=yaxes(left_format=UNITS.COUNTS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_apply_cache_event",
                            label_selectors=[],
                            by_labels=["type", "instance"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Overall RPC Duration",
                description=None,
                yaxis=yaxis(format=UNITS.SECONDS, log_base=1),
                metric="tikv_import_rpc_duration_bucket",
                label_selectors=[
                    'request="apply"',
                ],
            ),
            heatmap_panel(
                title="Read File into Memory Duration",
                description=None,
                yaxis=yaxis(format=UNITS.SECONDS, log_base=1),
                metric="tikv_import_apply_duration_bucket",
                label_selectors=[
                    'type="exec_download"',
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Queuing Time",
                description=None,
                yaxis=yaxis(format=UNITS.SECONDS, log_base=1),
                metric="tikv_import_engine_request_bucket",
                label_selectors=[
                    'type="queuing"',
                ],
            ),
            graph_panel(
                title="Apply Request Throughput",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_apply_bytes_sum",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Downloaded File Size",
                description=None,
                yaxis=yaxis(format=UNITS.BYTES_IEC),
                metric="tikv_import_download_bytes_bucket",
            ),
            heatmap_panel(
                title="Apply Batch Size",
                description=None,
                yaxis=yaxis(format=UNITS.BYTES_IEC),
                metric="tikv_import_apply_bytes_bucket",
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Blocked by Concurrency Time",
                description=None,
                yaxis=yaxis(format=UNITS.SECONDS, log_base=1),
                metric="tikv_import_engine_request_bucket",
                label_selectors=[
                    'type="get_permit"',
                ],
            ),
            graph_panel(
                title="Apply Request Speed",
                description=None,
                yaxes=yaxes(
                    left_format=UNITS.OPS_PER_SEC,
                    log_base=1,
                ),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_applier_event",
                            label_selectors=[
                                'type="begin_req"',
                            ],
                            by_labels=["instance", "type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Cached File in Memory",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_IEC, log_base=1),
                targets=[
                    target(
                        expr=expr_sum("tikv_import_apply_cached_bytes"),
                    ),
                ],
            ),
            graph_panel(
                title="Engine Requests Unfinished",
                description=None,
                yaxes=yaxes(
                    left_format=UNITS.SHORT,
                    log_base=1,
                ),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_applier_event",
                            label_selectors=[
                                'type!="begin_req"',
                            ],
                            by_labels=["instance", "type"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Apply Time",
                description=None,
                yaxis=yaxis(format=UNITS.SECONDS, log_base=1),
                metric="tikv_import_engine_request_bucket",
                label_selectors=[
                    'type="apply"',
                ],
            ),
            graph_panel(
                title="Raft Store Memory Usage",
                description="",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC, log_base=1),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_server_mem_trace_sum",
                            label_selectors=[
                                'name=~"raftstore-.*"',
                            ],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def ResolvedTS() -> RowPanel:
    layout = Layout(title="Resolved TS")
    layout.row(
        [
            graph_panel(
                title="Resolved TS Worker CPU",
                description="The CPU utilization of resolved ts worker",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"resolved_ts.*"',
                            ],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Advance ts Worker CPU",
                description="The CPU utilization of advance ts worker",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"advance_ts.*"',
                            ],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Scan lock Worker CPU",
                description="The CPU utilization of scan lock worker",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"inc_scan.*"',
                            ],
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Max gap of resolved-ts",
                description="The gap between resolved ts (the maximum candidate of safe-ts) and current time.",
                yaxes=yaxes(left_format=UNITS.MILLI_SECONDS),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_resolved_ts_gap_millis",
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Min Resolved TS Region",
                description="The region that has minimal resolved ts",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_resolved_ts_region",
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Max gap of follower safe-ts",
                description="The gap between now() and the minimal (non-zero) safe ts for followers",
                yaxes=yaxes(left_format=UNITS.MILLI_SECONDS),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_follower_safe_ts_gap_millis",
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Min Safe TS Follower Region",
                description="The region id of the follower that has minimal safe ts",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_follower_safe_ts_region",
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Max gap of resolved-ts in region leaders",
                description="The gap between resolved ts of leaders and current time",
                yaxes=yaxes(left_format=UNITS.MILLI_SECONDS),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_leader_resolved_ts_gap_millis",
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Min Leader Resolved TS Region",
                description="The region that its leader has minimal resolved ts.",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_min_leader_resolved_ts_region",
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Check leader duration",
                description="The time consumed when handle a check leader request",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_resolved_ts_check_leader_duration_seconds_bucket",
            ),
            graph_panel(
                title="99% CheckLeader request region count",
                description="Bucketed histogram of region count in a check leader request",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_check_leader_request_item_count",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="99% CheckLeader request size",
                description="Bucketed histogram of the check leader request size",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_check_leader_request_size_bytes",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_check_leader_request_item_count",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}-check-num",
                    ),
                ],
            ),
            graph_panel(
                title="Fail advance ts count",
                description="The count of fail to advance resolved-ts",
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_resolved_ts_fail_advance_count",
                            by_labels=["instance", "reason"],
                        ),
                    ),
                    target(
                        expr=expr_sum_delta(
                            "tikv_raftstore_check_stale_peer",
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}-stale-peer",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Lock heap size",
                description="Total bytes in memory of resolved-ts observe regions's lock heap",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_resolved_ts_lock_heap_bytes",
                        ),
                    )
                ],
            ),
            heatmap_panel(
                title="Initial scan backoff duration",
                description="The backoff duration before starting initial scan",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_resolved_ts_initial_scan_backoff_duration_seconds_bucket",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Observe region status",
                description="The status of resolved-ts observe regions",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_resolved_ts_region_resolve_status",
                            by_labels=["type"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Pending command size",
                description="Total bytes of pending commands in the channel",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_avg(
                            "tikv_resolved_ts_channel_penging_cmd_bytes_total",
                        ),
                    )
                ],
            ),
        ]
    )
    return layout.row_panel


def Memory() -> RowPanel:
    layout = Layout(title="Memory")
    layout.row(
        [
            graph_panel(
                title="Allocator Stats",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_allocator_stats", by_labels=["instance", "type"]
                        )
                    )
                ],
            ),
            graph_panel(
                title="Send Allocated(+) / Release Received(-) Bytes Rate",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_allocator_thread_allocation",
                                label_selectors=['type="alloc"'],
                                by_labels=["thread_name"],
                            ),
                            "-",
                            expr_sum_rate(
                                "tikv_allocator_thread_allocation",
                                label_selectors=['type="dealloc"'],
                                by_labels=["thread_name"],
                            ),
                        ),
                        legend_format="{{thread_name}}",
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Newly Allocated Bytes by Thread",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_allocator_thread_allocation",
                            label_selectors=['type="alloc"'],
                            by_labels=["thread_name"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Recently Released Bytes by Thread",
                description=None,
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_allocator_thread_allocation",
                            label_selectors=['type="dealloc"'],
                            by_labels=["thread_name"],
                        ),
                    )
                ],
            ),
        ]
    )
    return layout.row_panel


def BackupImport() -> RowPanel:
    layout = Layout(title="Backup & Import")
    layout.row(
        [
            graph_panel(
                title="Backup CPU Utilization",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"b.*k.*w.*k.*"',
                            ],
                        ),
                        legend_format="backup-{{instance}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"backup_io"',
                            ],
                        ),
                        legend_format="backup-io-{{instance}}",
                    ),
                    target(
                        expr=expr_simple(
                            "tikv_backup_softlimit",
                        ),
                        legend_format="backup-auto-throttle-{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Backup Thread Count",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_backup_thread_pool_size",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Backup Errors",
                description="",
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_backup_error_counter",
                            by_labels=["instance", "error"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Backup Write CF SST Size",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
                metric="tikv_backup_range_size_bytes_bucket",
                label_selectors=['cf="write"'],
            ),
            heatmap_panel(
                title="Backup Default CF SST Size",
                yaxis=yaxis(format=UNITS.BYTES_IEC),
                metric="tikv_backup_range_size_bytes_bucket",
                label_selectors=['cf="default"'],
            ),
            graph_panel(
                title="Backup SST Generation Throughput",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_backup_range_size_bytes_sum",
                            by_labels=[],  # override default by instance.
                        ),
                        legend_format="total",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_backup_range_size_bytes_sum",
                            by_labels=["instance", "cf"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Backup Scan SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_backup_range_duration_seconds_bucket",
                label_selectors=['type="snapshot"'],
            ),
            heatmap_panel(
                title="Backup Scan SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_backup_range_duration_seconds_bucket",
                label_selectors=['type="scan"'],
            ),
            heatmap_panel(
                title="Backup Save SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_backup_range_duration_seconds_bucket",
                label_selectors=['type=~"save.*"'],
            ),
            graph_panel(
                title="Backup SST Duration",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.999,
                            "tikv_backup_range_duration_seconds",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}-99.9%",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_backup_range_duration_seconds",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}-99%",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum(
                                "tikv_backup_range_duration_seconds_sum",
                                by_labels=["type"],
                            ),
                            "/",
                            expr_sum(
                                "tikv_backup_range_duration_seconds_count",
                                by_labels=["type"],
                            ),
                        ),
                        legend_format="{{type}}-avg",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="External Storage Create Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_external_storage_create_seconds_bucket",
            ),
            graph_panel_histogram_quantiles(
                title="External Storage Create Duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_external_storage_create_seconds",
                hide_avg=True,
                hide_count=True,
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Checksum Request Duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_coprocessor_request_duration_seconds",
                label_selectors=['req=~"analyze.*|checksum.*"'],
                by_labels=["req"],
                hide_avg=True,
                hide_count=True,
            ),
            graph_panel(
                title="IO Utilization",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "node_disk_io_time_seconds_total",
                            by_labels=["instance", "device"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Import CPU Utilization",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sst_.*"'],
                            by_labels=["instance"],
                        ),
                        legend_format="import-{{instance}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sst_.*"'],
                            by_labels=["instance", "tid"],
                        ).extra("> 0"),
                        legend_format="import-{{instance}}-{{tid}}",
                        hide=True,
                    ),
                    target(
                        expr=expr_count_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sst_.*"'],
                        ),
                        legend_format="import-count-{{instance}}",
                        hide=True,
                    ),
                ],
            ),
            graph_panel(
                title="Import Thread Count",
                targets=[
                    target(
                        expr=expr_count_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=['name=~"sst_.*"'],
                            by_labels=["instance"],
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Import Errors",
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_import_error_counter",
                            by_labels=["type", "error", "instance"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Import RPC Duration",
                description="",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_import_rpc_duration",
                by_labels=["request"],
                hide_count=True,
            ),
            graph_panel(
                title="Import RPC Ops",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_rpc_duration_count",
                            label_selectors=['request!="switch_mode"'],
                            by_labels=["request"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Import RPC Count",
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_import_rpc_count",
                        ),
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Import Write/Download RPC Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_rpc_duration_bucket",
                label_selectors=['request=~"download|write"'],
            ),
            heatmap_panel(
                title="Import Wait Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_download_duration_bucket",
                label_selectors=['type="queue"'],
            ),
            heatmap_panel(
                title="Import Read SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_download_duration_bucket",
                label_selectors=['type="read"'],
            ),
            heatmap_panel(
                title="Import Rewrite SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_download_duration_bucket",
                label_selectors=['type="rewrite"'],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Import Ingest RPC Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_rpc_duration_bucket",
                label_selectors=['request=~"ingest"'],
            ),
            heatmap_panel(
                title="Import Ingest SST Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_ingest_duration_bucket",
                label_selectors=['type=~"ingest"'],
            ),
            heatmap_panel(
                title="Import Ingest SST Bytes",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_import_ingest_byte_bucket",
            ),
            graph_panel(
                title="Import Download SST Throughput",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_download_bytes_sum",
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_download_bytes_sum",
                            by_labels=[],
                        ),
                        legend_format="total",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Import Local Write keys",
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_import_local_write_keys",
                            by_labels=["type", "instance"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Import Local Write bytes",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_import_local_write_bytes",
                            by_labels=["type", "instance"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="TTL Expired",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_backup_raw_expired_count",
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_backup_raw_expired_count",
                            by_labels=[],
                        ),
                        legend_format="sum",
                    ),
                ],
            ),
            graph_panel(
                title="cloud request",
                description="",
                yaxes=yaxes(left_format=UNITS.SHORT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_cloud_request_duration_seconds_count",
                            by_labels=["cloud", "req"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def Encryption() -> RowPanel:
    layout = Layout(title="Encryption")
    layout.row(
        [
            graph_panel(
                title="Encryption data keys",
                description="Total number of encryption data keys in use",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_encryption_data_key_storage_total",
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Encrypted files",
                description="Number of files being encrypted",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_encryption_file_num",
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Encryption initialized",
                description="Flag to indicate if encryption is initialized",
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_encryption_is_initialized",
                        ),
                        legend_format="{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Encryption meta files size",
                description="Total size of encryption meta files",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_simple(
                            "tikv_encryption_meta_file_size_bytes",
                        ),
                        legend_format="{{name}}-{{instance}}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Encrypt/decrypt data nanos",
                description="",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            label_selectors=[
                                'metric="encrypt_data_nanos"',
                            ],
                            by_labels=["req"],
                        ),
                        legend_format="encrypt-{{req}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            label_selectors=[
                                'metric="decrypt_data_nanos"',
                            ],
                            by_labels=["req"],
                        ),
                        legend_format="decrypt-{{req}}",
                    ),
                ],
            ),
            graph_panel_histogram_quantiles(
                title="Read/write encryption meta duration",
                description="Writing or reading file duration (second)",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                metric="tikv_encryption_write_read_file_duration_seconds",
                hide_count=True,
            ),
        ]
    )
    return layout.row_panel


def BackupLog() -> RowPanel:
    layout = Layout(title="Backup Log")
    layout.row(
        [
            stat_panel(
                title="Endpoint Status",
                targets=[
                    target(
                        expr=expr_simple("tikv_log_backup_enabled"),
                        legend_format="{{ instance }}",
                    ),
                ],
                mappings=[
                    StatValueMappings(
                        StatValueMappingItem("Disabled", "0", "red"),
                        StatValueMappingItem("Enabled", "1", "green"),
                    ),
                ],
            ),
            stat_panel(
                title="Task Status",
                targets=[
                    target(
                        expr=expr_min("tikv_log_backup_task_status"),
                    ),
                ],
                mappings=[
                    StatValueMappings(
                        StatValueMappingItem("Running", "0", "green"),
                        StatValueMappingItem("Paused", "1", "yellow"),
                        StatValueMappingItem("Error", "2", "red"),
                    ),
                ],
            ),
            stat_panel(
                title="Advancer Owner",
                text_mode="name",
                targets=[
                    target(
                        expr="tidb_log_backup_advancer_owner > 0",
                        legend_format="{{ instance }}",
                    ),
                ],
            ),
            stat_panel(
                title="Average Flush Size",
                description="The average flush size of last 30mins.",
                format=UNITS.BYTES_IEC,
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_increase(
                                "tikv_log_backup_flush_file_size_sum",
                                range_selector="30m",
                            ),
                            "/",
                            expr_sum_increase(
                                "tikv_log_backup_flush_duration_sec_count",
                                label_selectors=['stage=~"save_files"'],
                                range_selector="30m",
                            ),
                        ),
                        legend_format="{{ instance }}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            stat_panel(
                title="Flushed Files (Last 30m) Per Host",
                description="The current total flushed file number of this run.",
                decimals=0,
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_flush_file_size_count",
                            range_selector="30m",
                        ).extra("> 0"),
                    ),
                ],
            ),
            stat_panel(
                title="Flush Times (Last 30m)",
                description="This is the summary of the file count has been flushed, summered by the data each TiKV has flushed since last boot.\n**NOTE: The size may get reduced if some of TiKVs reboot.**",
                decimals=0,
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_flush_duration_sec_count",
                            range_selector="30m",
                            label_selectors=['stage=~"save_files"'],
                        ),
                    ),
                ],
            ),
            stat_panel(
                title="Total Flushed Size (Last 30m)",
                description="This is the summary of the size has been flushed, summered by the data each TiKV has flushed since last boot.\n**NOTE: The size may get reduced if some of TiKVs reboot.**",
                format=UNITS.BYTES_IEC,
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_flush_file_size_sum",
                            range_selector="30m",
                        ),
                    ),
                ],
            ),
            stat_panel(
                title="Flush Files (Last 30m)",
                description="This is the summary of the file count has been flushed, summered by the data each TiKV has flushed since last boot.\n**NOTE: The size may get reduced if some of TiKVs reboot.**",
                decimals=0,
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_flush_file_size_count",
                            range_selector="30m",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="CPU Usage",
                description="The CPU utilization of log backup threads. \n**(Note this is the average usage for a period of time, some peak of CPU usage may be lost.)**",
                yaxes=yaxes(left_format=UNITS.PERCENT_UNIT),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_thread_cpu_seconds_total",
                            label_selectors=[
                                'name=~"backup_stream|log-backup-scan(-[0-9]+)?"'
                            ],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Handle Event Rate",
                description="",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_handle_kv_batch_sum",
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Initial Scan Generate Event Throughput",
                description="The data rate of initial scanning emitting events.",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_incremental_scan_bytes_sum",
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Abnormal Checkpoint TS Lag",
                description=None,
                yaxes=yaxes(left_format=UNITS.MILLI_SECONDS),
                targets=[
                    target(
                        expr=expr_operator(
                            "time() * 1000",
                            "-",
                            expr_max(
                                "tidb_log_backup_last_checkpoint", by_labels=["task"]
                            ).extra("/ 262144 > 0"),
                        ),
                        legend_format="{{ task }}",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Memory Of Events",
                description="The estimated memory usage by the streaming backup module.",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum("tikv_log_backup_heap_memory"),
                    )
                ],
            ),
            graph_panel(
                title="Observed Region Count",
                description="",
                targets=[
                    target(
                        expr=expr_sum("tikv_log_backup_observed_region"),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_log_backup_observed_region",
                        ),
                        legend_format="{{instance}}-total",
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Errors",
                description="The errors met when backing up.\n**They are retryable, don't worry.**",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_errors",
                            range_selector="1m",
                            by_labels=["type", "instance"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Fatal Errors",
                description="The errors met when backing up.",
                yaxes=yaxes(left_format=UNITS.OPS_PER_MIN),
                targets=[
                    target(
                        expr=expr_sum_delta(
                            "tikv_log_backup_fatal_errors",
                            range_selector="1m",
                            by_labels=["type", "instance"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Checkpoint TS of Tasks",
                description=None,
                yaxes=yaxes(left_format=UNITS.DATE_TIME_ISO_TODAY),
                null_point_mode=NULL_AS_NULL,
                targets=[
                    target(
                        expr=expr_max(
                            "tidb_log_backup_last_checkpoint", by_labels=["task"]
                        ).extra("/ 262144 > 0"),
                    ),
                    target(expr="time() * 1000", legend_format="Current Time"),
                ],
                series_overrides=[
                    series_override(
                        alias="Current Time",
                        fill=0,
                        dashes=True,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Flush Duration",
                description="The duration of flushing a batch of file.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_flush_duration_sec_bucket",
                label_selectors=['stage=~"save_files"'],
            ),
            heatmap_panel(
                title="Initial scanning duration",
                description="The duration of scanning the initial data from local DB and transform them into apply events.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_initial_scan_duration_sec_bucket",
            ),
            heatmap_panel(
                title="Convert Raft Event duration",
                description="The duration of converting a raft request into a apply event.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_event_handle_duration_sec_bucket",
                label_selectors=['stage=~"to_stream_event"'],
            ),
            heatmap_panel(
                title="Wait for Lock Duration",
                description="The duration of waiting the mutex of the controller.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_event_handle_duration_sec_bucket",
                label_selectors=['stage=~"get_router_lock"'],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Command Batch Size",
                description="The number of KV-modify of each raft command observed.",
                yaxis=yaxis(format=UNITS.SHORT),
                metric="tikv_log_backup_handle_kv_batch_bucket",
            ),
            heatmap_panel(
                title="Save to Temp File Duration",
                description="The total cost of saving an event into temporary file.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_event_handle_duration_sec_bucket",
                label_selectors=['stage=~"save_to_temp_file"'],
            ),
            heatmap_panel(
                title="Write to Temp File Duration",
                description="The total cost of writing a event into temporary file.\nComparing to the ***Save*** duration, it doesn't contain the time cost of routing the task by range / task.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_on_event_duration_seconds_bucket",
                label_selectors=['stage="write_to_tempfile"'],
            ),
            heatmap_panel(
                title="System Write Call Duration",
                description="The duration of collecting metadata and call the UNIX system call *write* for each event.",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tikv_log_backup_on_event_duration_seconds_bucket",
                label_selectors=['stage="syscall_write"'],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Internal Message Type",
                description="The internal message type count.",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC, log_base=2),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_interal_actor_acting_duration_sec_count",
                            by_labels=["message"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Internal Message Handling Duration (P99)",
                description="The internal handling message duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_log_backup_interal_actor_acting_duration_sec",
                            by_labels=["message"],
                        ),
                        legend_format="{{message}}",
                    )
                ],
            ),
            graph_panel(
                title="Internal Message Handling Duration (P90)",
                description="The internal handling message duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9,
                            "tikv_log_backup_interal_actor_acting_duration_sec",
                            by_labels=["message"],
                        ),
                        legend_format="{{message}}",
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Initial Scan RocksDB Throughput",
                description="The internal read throughput of RocksDB during initial scanning. This panel can roughly present the read through to the hard disk of initial scanning.",
                yaxes=yaxes(left_format=UNITS.BYTES_SEC_IEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_initial_scan_operations",
                            label_selectors=['op=~"read_bytes"'],
                            by_labels=["cf"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Initial Scan RocksDB Operation",
                description="Misc statistics of RocksDB during initial scanning.",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_initial_scan_operations",
                            label_selectors=['op!~"read_bytes"'],
                            by_labels=["cf", "op"],
                        ).extra("> 0"),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Initial Scanning Trigger Reason",
                description="The reason of triggering initial scanning.",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_initial_scan_reason",
                            by_labels=["reason"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Region Checkpoint Key Putting",
                description="",
                yaxes=yaxes(left_format=UNITS.COUNTS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_log_backup_metadata_key_operation",
                            by_labels=["type"],
                        ),
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Request Checkpoint Batch Size",
                metric="tidb_log_backup_advancer_batch_size_bucket",
                label_selectors=['type="checkpoint"'],
            ),
            heatmap_panel(
                title="Tick Duration",
                yaxis=yaxis(format=UNITS.SECONDS),
                metric="tidb_log_backup_advancer_tick_duration_sec_bucket",
                label_selectors=['step="tick"'],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Region Checkpoint Failure Reason",
                description="The reason of advancer failed to be advanced.",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tidb_log_backup_region_request_failure",
                            label_selectors=['reason!="retryable-scan-region"'],
                            by_labels=["reason"],
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Request Result",
                description="The result of getting region checkpoints.",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tidb_log_backup_region_request",
                            by_labels=["result"],
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Tick Duration (P99)",
                description="The internal handling message duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tidb_log_backup_advancer_tick_duration_sec",
                            by_labels=["step"],
                        ),
                        legend_format="{{ step }}",
                    )
                ],
            ),
            graph_panel(
                title="Tick Duration (P90)",
                description="The internal handling message duration.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.9,
                            "tidb_log_backup_advancer_tick_duration_sec",
                            by_labels=["step"],
                        ),
                        legend_format="{{ step }}",
                    )
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Get Region Operation Count",
                description="The frequent of getting region level checkpoint.",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tidb_log_backup_advancer_tick_duration_sec_count",
                            label_selectors=['step="get-regions-in-range"'],
                            by_labels=["step", "instance"],
                        ),
                    )
                ],
            ),
            graph_panel(
                title="Try Advance Trigger Time",
                description="The variant of checkpoint group.",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tidb_log_backup_advancer_tick_duration_sec_count",
                            label_selectors=['step="try-advance"'],
                            by_labels=["step", "instance"],
                        ),
                    )
                ],
            ),
        ]
    )
    return layout.row_panel


def SlowTrendStatistics() -> RowPanel:
    layout = Layout(title="Slow Trend Statistics")
    layout.row(
        [
            graph_panel(
                title="Inspected duration per server",
                description="The duration that recorded by inspecting messages.",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_inspect_duration_seconds",
                            by_labels=["instance", "type"],
                        ),
                        legend_format="{{instance}}-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Store Slow Score",
                description="The slow score of stores",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_slow_score",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Slow Trend",
                description="The changing trend of the slowness on I/O operations. 'value > 0' means the related store might have a slow trend.",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_slow_trend",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="QPS Changing Trend",
                description="The changing trend of QPS on each store. 'value < 0' means the QPS has a dropping trend.",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_slow_trend_result",
                        ),
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="AVG Sampling Latency",
                description="The sampling latency of recent queries. A larger value indicates that the store is more likely to be the slowest store.",
                yaxes=yaxes(left_format=UNITS.MICRO_SECONDS),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_slow_trend_l0",
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="QPS of each store",
                description="The QPS of each store.",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_slow_trend_result_value",
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def StatusServer() -> RowPanel:
    layout = Layout(title="Status Server")
    layout.row(
        [
            graph_panel_histogram_quantiles(
                title="Status API Request Duration",
                description="The 99 quantile durtion of status server API requests",
                metric="tikv_status_server_request_duration_seconds",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                by_labels=["path"],
                hide_p9999=True,
                hide_count=True,
                hide_avg=True,
            ),
            graph_panel(
                title="Status API Request (op/s)",
                yaxes=yaxes(left_format=UNITS.OPS_PER_SEC),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_status_server_request_duration_seconds_count",
                            by_labels=["path"],
                        ),
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


#### Metrics Definition End ####


dashboard = Dashboard(
    title="Test-Cluster-TiKV-Details",
    uid="RDVQiEzZz",
    timezone="browser",
    refresh="1m",
    inputs=[DATASOURCE_INPUT],
    editable=True,
    templating=Templates(),
    panels=[
        # Overview
        Duration(),
        Cluster(),
        Errors(),
        Server(),
        # Entrance of Write and Read
        gRPC(),
        Storage(),
        LocalReader(),
        # CPU and IO
        ThreadCPU(),
        IOBreakdown(),
        # Raftstore
        RaftWaterfall(),
        RaftIO(),
        RaftPropose(),
        RaftProcess(),
        RaftMessage(),
        RaftAdmin(),
        RaftLog(),
        # Engine
        RaftEngine(),
        RocksDB(),
        Titan(),
        RangeCacheMemoryEngine(),
        # Scheduler and Read Pools
        FlowControl(),
        Scheduler(),
        SchedulerCommands(),
        CoprocessorOverview(),
        CoprocessorDetail(),
        UnifiedReadPool(),
        # Transaction
        GC(),
        PessimisticLocking(),
        # Background Tasks
        Task(),
        PD(),
        SlowTrendStatistics(),
        Snapshot(),
        # Tools
        ResolvedTS(),
        PointInTimeRestore(),
        BackupImport(),
        BackupLog(),
        # Advanced Debugging for CPU and Memory
        Threads(),
        Memory(),
        # Infrequently Used
        StatusServer(),
        Encryption(),
        TTL(),
    ],
    # Set 14 or larger to support shared crosshair or shared tooltip.
    # See https://github.com/grafana/grafana/blob/v10.2.2/public/app/features/dashboard/state/DashboardMigrator.ts#L443-L445
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
).auto_panel_ids()
