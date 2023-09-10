from grafanalib import formatunits as UNITS
from grafanalib.core import (
    GRAPH_TOOLTIP_MODE_SHARED_TOOLTIP,
    HIDE_VARIABLE,
    SHOW,
    Dashboard,
    DataSourceInput,
    Graph,
    GridPos,
    Legend,
    Panel,
    RowPanel,
    Target,
    Template,
    Templating,
    TimeSeries,
    Tooltip,
    YAxes,
    YAxis,
)

DATASOURCE_INPUT = DataSourceInput(
    name="DS_TEST-CLUSTER",
    label="test-cluster",
    pluginId="prometheus",
    pluginName="Prometheus",
)
DATASOURCE = f"${{{DATASOURCE_INPUT.name}}}"

#### Utilities Function Start ####


def template(
    name, query, dataSource, hide, regex=None, includeAll=False, allValue=None
) -> Template:
    return Template(
        dataSource=dataSource,
        hide=hide,
        label=name,
        multi=False,
        name=name,
        query=query,
        refresh=2,
        sort=1,
        type="query",
        useTags=False,
        regex=regex,
        includeAll=includeAll,
        allValue=allValue,
    )


class Layout:
    # Rows are always 24 "units" wide.
    ROW_WIDTH = 24
    PANEL_HEIGHT = 6
    row_panel: RowPanel
    current_row_y_pos: int

    def __init__(self, title, collapsed=True) -> None:
        self.current_row_y_pos = 0
        self.row_panel = RowPanel(
            title=title,
            gridPos=GridPos(h=self.PANEL_HEIGHT, w=self.ROW_WIDTH, x=0, y=0),
            collapsed=collapsed,
        )

    def row(self, panels: list[Panel]):
        """Start a new row and evenly scales panels width"""
        count = len(panels)
        if count == 0:
            return panels
        width = self.ROW_WIDTH // count
        remain = self.ROW_WIDTH % count
        x = 0
        for panel in panels:
            panel.gridPos = GridPos(
                h=self.PANEL_HEIGHT,
                w=width,
                x=x,
                y=self.current_row_y_pos,
            )
            x += width
        panels[-1].gridPos.w += remain
        self.row_panel.panels.extend(panels)
        self.current_row_y_pos += self.PANEL_HEIGHT


def target(expr, legendFormat, hide=False) -> Target:
    return Target(
        expr=expr,
        hide=hide,
        legendFormat=legendFormat,
        intervalFactor=1,  # Prefer "high" resolution
    )


def timeseries_panel(
    title,
    targets,
    legendCalcs=["max", "last"],
    unit="s",
    drawStyle="line",
    lineWidth=1,
    fillOpacity=10,
    gradientMode="opacity",
    tooltipMode="multi",
    legendDisplayMode="table",
    legendPlacement="right",
    description=None,
    dataSource=DATASOURCE,
) -> TimeSeries:
    return TimeSeries(
        title=title,
        dataSource=dataSource,
        description=description,
        targets=targets,
        legendCalcs=legendCalcs,
        drawStyle=drawStyle,
        lineWidth=lineWidth,
        fillOpacity=fillOpacity,
        gradientMode=gradientMode,
        unit=unit,
        tooltipMode=tooltipMode,
        legendDisplayMode=legendDisplayMode,
        legendPlacement=legendPlacement,
    )


def graph_legend(
    avg=False,
    current=True,
    max=True,
    min=False,
    show=True,
    total=False,
    alignAsTable=True,
    hideEmpty=True,
    hideZero=True,
    rightSide=True,
    sideWidth=None,
    sortDesc=True,
) -> Legend:
    sort = "max" if max else "current"
    return Legend(
        avg=avg,
        current=current,
        max=max,
        min=min,
        show=show,
        total=total,
        alignAsTable=alignAsTable,
        hideEmpty=hideEmpty,
        hideZero=hideZero,
        rightSide=rightSide,
        sideWidth=sideWidth,
        sort=sort,
        sortDesc=sortDesc,
    )


def graph_panel(
    title: str,
    targets: list[Target],
    yaxes=YAxes(),
    legend=None,
    tooltip=Tooltip(shared=True, valueType="individual"),
    lines=True,
    lineWidth=1,
    fill=0,
    fillGradient=0,
    stack=False,
    description=None,
    dataSource=DATASOURCE,
) -> Panel:
    # extraJson add patches grafanalib result.
    extraJson = {}
    if fillGradient != 0:
        # fillGradient is only valid when fill is 1.
        if fill == 0:
            fill = 1
        # fillGradient is not set correctly in grafanalib(0.7.0), so we need to
        # set it manually.
        # TODO: remove it when grafanalib fix this.
        extraJson["fillGradient"] = 1

    return Graph(
        title=title,
        dataSource=dataSource,
        description=description,
        targets=targets,
        yAxes=yaxes,
        legend=legend if legend else graph_legend(),
        lines=lines,
        bars=not lines,
        lineWidth=lineWidth,
        fill=fill,
        fillGradient=fillGradient,
        stack=stack,
        tooltip=tooltip,
        # Do not specify max max data points, let Grafana decide.
        maxDataPoints=None,
        extraJson=extraJson,
    )


def expr_sum_rate(
    metrics: str,
    labels_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
    by_labels: list[str] = ["instance"],
    default_labels_selectors: list[str] = [
        r'k8s_cluster="$k8s_cluster"',
        r'tidb_cluster="$tidb_cluster"',
        r'instance=~"$instance"',
    ],
    extra_expr: str = "",
) -> str:
    """
    Calculate the sum of rate of metrics.

    Example:

        sum(rate(
            tikv_grpc_msg_duration_seconds_count
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
            [$__rate_interval]
        )) by (instance)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    instant_selectors = default_labels_selectors + labels_selectors
    return expr_aggregate_function(
        metrics, "sum", "rate", range_selector, instant_selectors, by_labels, extra_expr
    )


def expr_sum_delta(
    metrics: str,
    labels_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
    by_labels: list[str] = ["instance"],
    default_labels_selectors: list[str] = [
        r'k8s_cluster="$k8s_cluster"',
        r'tidb_cluster="$tidb_cluster"',
        r'instance=~"$instance"',
    ],
    extra_expr: str = "",
) -> str:
    """
    Calculate the sum of rate of metrics.

    Example:

        sum(delta(
            tikv_grpc_msg_duration_seconds_count
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
            [$__rate_interval]
        )) by (instance)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    instant_selectors = default_labels_selectors + labels_selectors
    return expr_aggregate_function(
        metrics,
        "sum",
        "delta",
        range_selector,
        instant_selectors,
        by_labels,
        extra_expr,
    )


def expr_sum(
    metrics: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
    default_labels_selectors: list[str] = [
        r'k8s_cluster="$k8s_cluster"',
        r'tidb_cluster="$tidb_cluster"',
        r'instance=~"$instance"',
    ],
    extra_expr: str = "",
) -> str:
    """
    Calculate the sum of metrics.

    Example:

        sum((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    instant_selectors = default_labels_selectors + labels_selectors
    return expr_aggregate_function(
        metrics=metrics,
        agg_opeator="sum",
        labels_selectors=instant_selectors,
        by_labels=by_labels,
        extra_expr=extra_expr,
    )


def expr_avg(
    metrics: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
    default_labels_selectors: list[str] = [
        r'k8s_cluster="$k8s_cluster"',
        r'tidb_cluster="$tidb_cluster"',
        r'instance=~"$instance"',
    ],
    extra_expr: str = "",
) -> str:
    """
    Calculate the avg of metrics.

    Example:

        avg((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    instant_selectors = default_labels_selectors + labels_selectors
    return expr_aggregate_function(
        metrics=metrics,
        agg_opeator="avg",
        labels_selectors=instant_selectors,
        by_labels=by_labels,
        extra_expr=extra_expr,
    )


def expr_aggregate_function(
    metrics: str,
    agg_opeator: str = "",
    func: str = "",
    range_selector: str = "",
    labels_selectors: list[str] = [],
    by_labels: list[str] = [],
    extra_expr: str = "",
) -> str:
    instant_selectors = (
        "{{{}}}".format(",".join(labels_selectors)) if labels_selectors else ""
    )
    range_selector = f"[{range_selector}]" if len(range_selector) != 0 else ""
    by_clause = "by ({})".format(", ".join(by_labels)) if by_labels else ""
    return f"""{agg_opeator}({func}(
    {metrics}
    {instant_selectors}
    {range_selector}
)) {by_clause} {extra_expr}"""


def expr_simple(
    metrics: str,
    labels_selectors: list[str] = [],
    default_labels_selectors: list[str] = [
        r'k8s_cluster="$k8s_cluster"',
        r'tidb_cluster="$tidb_cluster"',
        r'instance=~"$instance"',
    ],
    extra_expr: str = "",
) -> str:
    default_labels_selectors.extend(labels_selectors)
    instant_selectors = (
        "{{{}}}".format(",".join(default_labels_selectors))
        if default_labels_selectors
        else ""
    )
    return f"""{metrics}{instant_selectors}"""


def expr_opeator(lhs: str, operator: str, rhs: str):
    return f"""({lhs} {operator} {rhs})"""


def expr_histogram_quantile(
    quantile: float,
    metrics: str,
    labels_selectors: list[str] = [],
    k8s_cluster="$k8s_cluster",
    tidb_cluster="$tidb_cluster",
    instance="$instance",
) -> str:
    mlabels = (", " + ",".join(labels_selectors)) if labels_selectors else ""
    return f"""histogram_quantile({quantile}, sum(rate(
    {metrics}
    {{k8s_cluster="{k8s_cluster}", tidb_cluster="{tidb_cluster}", instance=~"{instance}"{mlabels}}}
    [$__rate_interval]
)) by (le))"""


#### Utilities Function End ####

#### Metrics Definition Start ####


def Templats() -> Templating:
    return Templating(
        [
            template(
                name="k8s_cluster",
                query="label_values(tikv_engine_block_cache_size_bytes, k8s_cluster)",
                dataSource=DATASOURCE,
                hide=HIDE_VARIABLE,
            ),
            template(
                name="tidb_cluster",
                query='label_values(tikv_engine_block_cache_size_bytes{k8s_cluster ="$k8s_cluster"}, tidb_cluster)',
                dataSource=DATASOURCE,
                hide=HIDE_VARIABLE,
            ),
            template(
                name="db",
                query='label_values(tikv_engine_block_cache_size_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, db)',
                dataSource=DATASOURCE,
                hide=SHOW,
            ),
            template(
                name="command",
                query='query_result(tikv_storage_command_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"} != 0)',
                dataSource=DATASOURCE,
                hide=SHOW,
                regex='/type="([^"]+)"/',
                includeAll=True,
            ),
            template(
                name="instance",
                query='label_values(tikv_engine_size_bytes{k8s_cluster ="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
                dataSource=DATASOURCE,
                hide=SHOW,
                includeAll=True,
            ),
            template(
                name="titan_db",
                query='label_values(tikv_engine_titandb_num_live_blob_file{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, db)',
                dataSource=DATASOURCE,
                hide=HIDE_VARIABLE,
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
                yaxes=YAxes(left=YAxis(format=UNITS.SECONDS)),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_append_log_duration_seconds_bucket"
                        ),
                        legendFormat="Write Raft Log .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_raftstore_request_wait_time_duration_secs_bucket",
                        ),
                        legendFormat="Propose Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_apply_wait_time_duration_secs_bucket"
                        ),
                        legendFormat="Apply Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_commit_log_duration_seconds_bucket"
                        ),
                        legendFormat="Replicate Raft Log .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_apply_log_duration_seconds_bucket"
                        ),
                        legendFormat="Apply Duration .99",
                    ),
                ],
            ),
            graph_panel(
                title="Cop Read Duration",
                description="Read Duration Composition",
                yaxes=YAxes(left=YAxis(format=UNITS.SECONDS)),
                lines=False,
                stack=True,
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_storage_engine_async_request_duration_seconds_bucket",
                            ['type="snapshot"'],
                        ),
                        legendFormat="Get Snapshot .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_coprocessor_request_wait_seconds_bucket"
                        ),
                        legendFormat="Cop Wait .99",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.95, "tikv_coprocessor_request_handle_seconds_bucket"
                        ),
                        legendFormat="Cop Handle .99",
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
                yaxes=YAxes(left=YAxis(format=UNITS.BYTES)),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes", labels_selectors=['type = "used"']
                        ),
                        legendFormat=r"{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Available size",
                description="The available capacity size of each TiKV instance",
                yaxes=YAxes(left=YAxis(format=UNITS.BYTES)),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes",
                            labels_selectors=['type="available"'],
                        ),
                        legendFormat=r"{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Capacity size",
                description="The capacity size per TiKV instance",
                yaxes=YAxes(left=YAxis(format=UNITS.BYTES)),
                fill=1,
                stack=True,
                legend=graph_legend(max=False),
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_store_size_bytes",
                            labels_selectors=['type="capacity"'],
                        ),
                        legendFormat=r"{{instance}}",
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
                yaxes=YAxes(left=YAxis(format=UNITS.PERCENT_UNIT)),
                targets=[
                    target(
                        expr=expr_sum_rate("process_cpu_seconds_total"),
                        legendFormat=r"{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Memory",
                description="The memory usage per TiKV instance",
                yaxes=YAxes(left=YAxis(format=UNITS.BYTES)),
                targets=[
                    target(
                        expr=expr_sum("process_resident_memory_bytes"),
                        legendFormat=r"{{instance}}",
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
                yaxes=YAxes(left=YAxis(format=UNITS.PERCENT_UNIT)),
                targets=[
                    target(
                        expr=expr_sum_rate("node_disk_io_time_seconds_total"),
                        legendFormat=r"{{instance}}-{{device}}",
                    ),
                ],
            ),
            graph_panel(
                title="MBps",
                description="The total bytes of read and write in each TiKV instance",
                yaxes=YAxes(left=YAxis(format=UNITS.BYTES)),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            labels_selectors=['type="wal_file_bytes"'],
                        ),
                        legendFormat=r"{{instance}}-write",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            labels_selectors=['type=~"bytes_read|iter_bytes_read"'],
                        ),
                        legendFormat=r"{{instance}}-read",
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
                yaxes=YAxes(left=YAxis(format=UNITS.OPS_PER_SEC)),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_duration_seconds_count",
                            labels_selectors=['type!="kv_gc"'],
                        ),
                        legendFormat=r"{{instance}}-{{type}}",
                    ),
                ],
            ),
            graph_panel(
                title="Errps",
                description="The total number of the gRPC message failures",
                yaxes=YAxes(left=YAxis(format=UNITS.OPS_PER_SEC)),
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_fail_total",
                            labels_selectors=['type!="kv_gc"'],
                        ),
                        legendFormat=r"{{instance}}-grpc-msg-fail",
                    ),
                    target(
                        expr=expr_sum_delta(
                            "tikv_pd_heartbeat_message_total",
                            labels_selectors=['type="noop"'],
                            extra_expr="< 1",
                        ),
                        legendFormat=r"{{instance}}-pd-heartbeat",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_critical_error_total", by_labels=["instance", "type"]
                        ),
                        legendFormat=r"{{instance}}-{{type}}",
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
                            labels_selectors=['type="leader"'],
                        ),
                        legendFormat=r"{{instance}}",
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
                            labels_selectors=['type="region"'],
                        ),
                        legendFormat=r"{{instance}}",
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_region_count",
                            labels_selectors=['type="buckets"'],
                        ),
                        legendFormat=r"{{instance}}-buckets",
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
                yaxes=YAxes(left=YAxis(format=UNITS.SECONDS)),
                fill=1,
                fillGradient=1,
                targets=[
                    target(
                        expr=expr_opeator(
                            "time()", "-", expr_simple("process_start_time_seconds")
                        ),
                        legendFormat=r"{{instance}}",
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
                description="TiKV uptime since the last restart",
                yaxes=YAxes(left=YAxis(format=UNITS.SECONDS)),
                fill=1,
                fillGradient=1,
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_critical_error_total", by_labels=["instance", "type"]
                        ),
                        legendFormat=r"{{instance}}-{{type}}",
                    ),
                ],
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
                        expr=expr_sum_rate("tikv_scheduler_too_busy_total"),
                        legendFormat=r"scheduler-{{instance}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_channel_full_total", by_labels=["instance", "type"]
                        ),
                        legendFormat=r"channelfull-{{instance}}-{{type}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_request_error",
                            labels_selectors=['type="full"'],
                        ),
                        legendFormat=r"coprocessor-{{instance}}",
                    ),
                    target(
                        expr=expr_avg(
                            "tikv_engine_write_stall",
                            labels_selectors=[
                                'type="write_stall_percentile99"',
                                'db=~"$db"',
                            ],
                            by_labels=["instance", "db"],
                        ),
                        legendFormat=r"stall-{{instance}}-{{db}}",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_store_write_msg_block_wait_duration_seconds_count"
                        ),
                        legendFormat=r"store-write-channelfull-{{instance}}",
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
                        legendFormat=r"{{instance}}-{{type}}-to-{{store_id}}",
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
                            labels_selectors=['status!~"success|all"'],
                            by_labels=["instance", "status"],
                        ),
                        legendFormat=r"{{instance}}-{{status}}",
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
                            labels_selectors=[
                                'stage=~"snapshot_err|prepare_write_err"'
                            ],
                            by_labels=["instance", "stage"],
                        ),
                        legendFormat=r"{{instance}}-{{stage}}",
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
                        legendFormat=r"{{instance}}-{{reason}}",
                    ),
                ],
            ),
            graph_panel(
                title="gRPC message error",
                description="The number of gRPC message errors per type on each TiKV instance",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_grpc_msg_fail_total", by_labels=["instance", "type"]
                        ),
                        legendFormat=r"{{instance}}-{{type}}",
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
                            labels_selectors=['type="leader"'],
                            by_labels=["instance"],
                        ),
                        legendFormat=r"{{instance}}",
                    ),
                ],
            ),
            graph_panel(
                title="Leader missing",
                description="The count of missing leaders per TiKV instance",
                targets=[
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_leader_missing", by_labels=["instance"]
                        ),
                        legendFormat=r"{{instance}}",
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
                        legendFormat=r"{{instance}}-existed",
                    ),
                    target(
                        expr=expr_simple("tikv_rocksdb_damaged_files_deleted"),
                        legendFormat=r"{{instance}}-deleted",
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
                            by_labels=["instance", "store_id"],
                        ),
                        legendFormat=r"{{instance}}-memory",
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


#### Metrics Definition End ####


dashboard = Dashboard(
    title="Test-Cluster-TiKV-Detailsaa",
    uid="RDVQiEzZzaa",
    timezone="browser",
    refresh="1m",
    inputs=[DATASOURCE_INPUT],
    editable=True,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_TOOLTIP,
    templating=Templats(),
    panels=[
        Duration(),
        Cluster(),
        Errors(),
    ],
).auto_panel_ids()
