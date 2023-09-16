from typing import Optional, Union

import attr
from attr.validators import in_, instance_of
from grafanalib import formatunits as UNITS
from grafanalib.core import (
    GRAPH_TOOLTIP_MODE_SHARED_TOOLTIP,
    HIDE_VARIABLE,
    SHOW,
    TIME_SERIES_TARGET_FORMAT,
    NULL_AS_NULL,
    Dashboard,
    DataSourceInput,
    Graph,
    GraphThreshold,
    GridPos,
    Heatmap,
    HeatmapColor,
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
    name, query, data_source, hide, regex=None, include_all=False, all_value=None
) -> Template:
    return Template(
        dataSource=data_source,
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
        includeAll=include_all,
        allValue=all_value,
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


def timeseries_panel(
    title,
    targets,
    legend_calcs=["max", "last"],
    unit="s",
    draw_style="line",
    line_width=1,
    fill_opacity=10,
    gradient_mode="opacity",
    tooltip_mode="multi",
    legend_display_mode="table",
    legend_placement="right",
    description=None,
    data_source=DATASOURCE,
) -> TimeSeries:
    return TimeSeries(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        legendCalcs=legend_calcs,
        drawStyle=draw_style,
        lineWidth=line_width,
        fillOpacity=fill_opacity,
        gradientMode=gradient_mode,
        unit=unit,
        tooltipMode=tooltip_mode,
        legendDisplayMode=legend_display_mode,
        legendPlacement=legend_placement,
    )


def graph_legend(
    avg=False,
    current=True,
    max=True,
    min=False,
    show=True,
    total=False,
    align_as_table=True,
    hide_empty=True,
    hide_zero=True,
    right_side=True,
    side_width=None,
    sort_desc=True,
) -> Legend:
    sort = "max" if max else "current"
    return Legend(
        avg=avg,
        current=current,
        max=max,
        min=min,
        show=show,
        total=total,
        alignAsTable=align_as_table,
        hideEmpty=hide_empty,
        hideZero=hide_zero,
        rightSide=right_side,
        sideWidth=side_width,
        sort=sort,
        sortDesc=sort_desc,
    )


def graph_panel(
    title: str,
    targets: list[Target],
    description=None,
    yaxes=YAxes(),
    legend=None,
    tooltip=Tooltip(shared=True, valueType="individual"),
    lines=True,
    line_width=1,
    fill=0,
    fill_gradient=0,
    stack=False,
    thresholds: list[GraphThreshold] = [],
    data_source=DATASOURCE,
) -> Panel:
    # extraJson add patches grafanalib result.
    extraJson = {}
    if fill_gradient != 0:
        # fillGradient is only valid when fill is 1.
        if fill == 0:
            fill = 1
        # fillGradient is not set correctly in grafanalib(0.7.0), so we need to
        # set it manually.
        # TODO: remove it when grafanalib fix this.
        extraJson["fillGradient"] = 1
    for target in targets:
        # Make sure traget is in time_series format.
        target.format = TIME_SERIES_TARGET_FORMAT

    return Graph(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        yAxes=yaxes,
        legend=legend if legend else graph_legend(),
        lines=lines,
        bars=not lines,
        lineWidth=line_width,
        fill=fill,
        fillGradient=fill_gradient,
        stack=stack,
        nullPointMode=NULL_AS_NULL,
        thresholds=thresholds,
        tooltip=tooltip,
        # Do not specify max max data points, let Grafana decide.
        maxDataPoints=None,
        extraJson=extraJson,
    )


def yaxis(format: str, log_base=1) -> YAxis:
    return YAxis(format=format, logBase=log_base)


def yaxes(left_format: str, right_format: Optional[str] = None, log_base=1) -> YAxes:
    ya = YAxes(left=yaxis(left_format, log_base=log_base))
    if right_format is not None:
        ya.right = yaxis(right_format, log_base=log_base)
    return ya


def heatmap_color() -> HeatmapColor:
    return HeatmapColor(
        cardColor="#b4ff00",
        colorScale="sqrt",
        colorScheme="interpolateSpectral",
        exponent=0.5,
        mode="spectrum",
        max=None,
        min=None,
    )


def heatmap_panel(
    title: str,
    targets: list[Target],
    description=None,
    yaxis=yaxis(UNITS.NO_FORMAT),
    tooltip=Tooltip(shared=True, valueType="individual"),
    color=heatmap_color(),
    data_source=DATASOURCE,
) -> Panel:
    for target in targets:
        assert (
            target.legendFormat == "{{le}}"
        ), f"Heatmap target must have legendFormat=le, got {target.legendFormat}"
        # Make sure targets are in heatmap format.
        target.format = "heatmap"

    return Heatmap(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        yAxis=yaxis,
        color=color,
        dataFormat="tsbuckets",
        yBucketBound="upper",
        tooltip=tooltip,
        extraJson={"tooltip": {"showHistogram": True}},
        hideZeroBuckets=True,
        # Do not specify max max data points, let Grafana decide.
        maxDataPoints=None,
    )


@attr.s
class Expr(object):
    """
    A prometheus expression that matches the following grammar:

    expr ::= <aggr_op> (
                [aggr_param,]
                [func](
                    <metric name>
                    [{<labels_selectors>,}]
                    [[<range_selector>]]
                )
            ) [by (<by_labels>,)] [extra_expr]
    """

    metric: str = attr.ib(validator=instance_of(str))
    aggr_op: str = attr.ib(
        default="",
        validator=in_(
            [
                "",
                "sum",
                "min",
                "max",
                "avg",
                "group",
                "stddev",
                "stdvar",
                "count",
                "count_values",
                "bottomk",
                "topk",
                "quantile",
            ]
        ),
    )
    aggr_param: str = attr.ib(default="", validator=instance_of(str))
    func: str = attr.ib(default="", validator=instance_of(str))
    range_selector: str = attr.ib(default="", validator=instance_of(str))
    labels_selectors: list[str] = attr.ib(default=[], validator=instance_of(list))
    by_labels: list[str] = attr.ib(default=[], validator=instance_of(list))
    default_labels_selectors: list[str] = attr.ib(
        default=[
            r'k8s_cluster="$k8s_cluster"',
            r'tidb_cluster="$tidb_cluster"',
            r'instance=~"$instance"',
        ],
        validator=instance_of(list),
    )
    extra_expr: str = attr.ib(default="", validator=instance_of(str))

    def __str__(self) -> str:
        aggr_opeator = self.aggr_op if self.aggr_op else ""
        aggr_param = self.aggr_param + "," if self.aggr_param else ""
        by_clause = (
            "by ({})".format(", ".join(self.by_labels)) if self.by_labels else ""
        )
        func = self.func if self.func else ""
        label_selectors = self.default_labels_selectors + self.labels_selectors
        instant_selectors = (
            "{{{}}}".format(",".join(label_selectors)) if label_selectors else ""
        )
        range_selector = f"[{self.range_selector}]" if self.range_selector else ""
        extra_expr = self.extra_expr if self.extra_expr else ""
        return f"""{aggr_opeator}({aggr_param}{func}(
    {self.metric}
    {instant_selectors}
    {range_selector}
)) {by_clause} {extra_expr}"""

    def aggregate(
        self,
        aggr_op: str,
        aggr_param: str = "",
        by_labels: list[str] = [],
        labels_selectors: list[str] = [],
    ) -> "Expr":
        self.aggr_op = aggr_op
        self.aggr_param = aggr_param
        self.by_labels = by_labels
        self.labels_selectors = labels_selectors
        return self

    def function(
        self,
        func: str,
        labels_selectors: list[str] = [],
        range_selector: str = "",
    ) -> "Expr":
        self.func = func
        self.labels_selectors = labels_selectors
        self.range_selector = range_selector
        return self

    def extra(
        self,
        extra_expr: Optional[str] = None,
        default_labels_selectors: Optional[list[str]] = None,
    ) -> "Expr":
        if extra_expr is not None:
            self.extra_expr = extra_expr
        if default_labels_selectors is not None:
            self.default_labels_selectors = default_labels_selectors
        return self


def expr_aggr(
    metric: str,
    aggr_op: str,
    aggr_param: str = "",
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the aggregation of a metric.

    Example:

        sum((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    expr = Expr(metric=metric)
    expr.aggregate(
        aggr_op,
        aggr_param=aggr_param,
        by_labels=by_labels,
        labels_selectors=labels_selectors,
    )
    return expr


def expr_sum(
    metric: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of a metric.

    Example:

        sum((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric, "sum", labels_selectors=labels_selectors, by_labels=by_labels
    )


def expr_avg(
    metric: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the avg of a metric.

    Example:

    avg((
        tikv_store_size_bytes
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
    )) by (instance)
    """
    return expr_aggr(
        metric, "avg", labels_selectors=labels_selectors, by_labels=by_labels
    )


def expr_aggr_func(
    metric: str,
    aggr_op: str,
    func: str,
    aggr_param: str = "",
    labels_selectors: list[str] = [],
    range_selector: str = "",
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the aggregation of function of a metric.

    Example:

    expr_aggr_func(
        tikv_grpc_msg_duration_seconds_count, "sum", "rate", lables_selectors=['type!="kv_gc"']
    )

    sum(rate(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    expr = Expr(metric=metric)
    expr.aggregate(
        aggr_op,
        aggr_param=aggr_param,
        by_labels=by_labels,
    )
    expr.function(
        func,
        labels_selectors=labels_selectors,
        range_selector=range_selector,
    )
    return expr


def expr_sum_rate(
    metric: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of rate of a metric.

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
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="rate",
        labels_selectors=labels_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
    )


def expr_sum_delta(
    metric: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of delta of a metric.

    Example:

    sum(delta(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="delta",
        labels_selectors=labels_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
    )


def expr_simple(
    metric: str,
    labels_selectors: list[str] = [],
) -> Expr:
    """
    Query an instant vector of a metric.

    Example:

    tikv_grpc_msg_duration_seconds_count
    {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
    """
    expr = Expr(metric=metric)
    expr.function("", labels_selectors=labels_selectors)
    return expr


def expr_operator(lhs: Union[Expr, str], operator: str, rhs: Union[Expr, str]) -> str:
    return f"""({lhs} {operator} {rhs})"""


def expr_histogram_quantile(
    quantile: float,
    metrics: str,
    labels_selectors: list[str] = [],
    by_labels: list[str] = [],
) -> Expr:
    """
    Query an instant vector of a metric.

    Example:

    histogram_quantile(0.99, sum(rate(
        tikv_grpc_msg_duration_seconds_bucket
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (le))
    """
    # sum(rate(metrics_bucket{label_selectors}[$__rate_interval])) by (le)
    by_labels = list(filter(lambda label: label != "le", by_labels))
    sum_rate_of_buckets = expr_sum_rate(
        metrics + "_bucket",
        labels_selectors=labels_selectors,
        by_labels=by_labels + ["le"],
    )
    # histogram_quantile({quantile}, {sum_rate_of_buckets})
    return expr_aggr(
        metric=f"{sum_rate_of_buckets}",
        aggr_op="histogram_quantile",
        aggr_param=f"{quantile}",
        labels_selectors=[],
        by_labels=[],
    ).extra(
        # Do not attach default label selector again.
        default_labels_selectors=[]
    )


def target(
    expr: Union[Expr, str],
    legend_format: Optional[str] = None,
    hide=False,
    data_source=DATASOURCE,
) -> Target:
    if legend_format is None and isinstance(expr, Expr) and expr.by_labels:
        legend_format = "-".join(map(lambda x: "{{" + f"{x}" + "}}", expr.by_labels))
    return Target(
        expr=f"{expr}",
        hide=hide,
        legendFormat=legend_format,
        intervalFactor=1,  # Prefer "high" resolution
        datasource=data_source,
    )


#### Utilities Function End ####

#### Metrics Definition Start ####


def Templats() -> Templating:
    return Templating(
        [
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
            ),
            template(
                name="command",
                query='query_result(tikv_storage_command_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"} != 0)',
                data_source=DATASOURCE,
                hide=SHOW,
                regex='/type="([^"]+)"/',
                include_all=True,
            ),
            template(
                name="instance",
                query='label_values(tikv_engine_size_bytes{k8s_cluster ="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
                data_source=DATASOURCE,
                hide=SHOW,
                include_all=True,
            ),
            template(
                name="titan_db",
                query='label_values(tikv_engine_titandb_num_live_blob_file{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, db)',
                data_source=DATASOURCE,
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
                            0.99, "tikv_coprocessor_request_wait_seconds"
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
                            labels_selectors=['type = "used"'],
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
                            labels_selectors=['type="available"'],
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
                            labels_selectors=['type="capacity"'],
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
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Memory",
                description="The memory usage per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_sum("process_resident_memory_bytes"),
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
                            labels_selectors=['type="wal_file_bytes"'],
                        ),
                        legend_format=r"{{instance}}-write",
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_engine_flow_bytes",
                            labels_selectors=['type=~"bytes_read|iter_bytes_read"'],
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
                            labels_selectors=['type!="kv_gc"'],
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
                            labels_selectors=['type!="kv_gc"'],
                        ),
                        legend_format=r"{{instance}}-grpc-msg-fail",
                    ),
                    target(
                        expr=expr_sum_delta(
                            "tikv_pd_heartbeat_message_total",
                            labels_selectors=['type="noop"'],
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
                            labels_selectors=['type="leader"'],
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
                            labels_selectors=['type="region"'],
                        ),
                    ),
                    target(
                        expr=expr_sum(
                            "tikv_raftstore_region_count",
                            labels_selectors=['type="buckets"'],
                        ),
                        legend_format=r"{{instance}}-buckets",
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
                yaxes=yaxes(left_format=UNITS.SECONDS),
                fill=1,
                fill_gradient=1,
                targets=[
                    target(
                        expr=expr_operator(
                            "time()", "-", expr_simple("process_start_time_seconds")
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
                description="TiKV uptime since the last restart",
                yaxes=yaxes(left_format=UNITS.SECONDS),
                fill=1,
                fill_gradient=1,
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
                            labels_selectors=['type="full"'],
                        ),
                        legend_format=r"coprocessor-{{instance}}",
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
                            labels_selectors=['status!~"success|all"'],
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
                            labels_selectors=[
                                'stage=~"snapshot_err|prepare_write_err"'
                            ],
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
                            labels_selectors=['type="leader"'],
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
            graph_panel(
                title="Approximate Region size",
                description="The approximate Region size",
                targets=[
                    target(
                        expr=expr_histogram_quantile(
                            0.99, "tikv_raftstore_region_size"
                        ),
                        legend_format="99%",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.95, "tikv_raftstore_region_size"
                        ),
                        legend_format="95%",
                    ),
                    target(
                        expr=expr_operator(
                            expr_sum_rate("tikv_raftstore_region_size_sum"),
                            "/",
                            expr_sum_rate("tikv_raftstore_region_size_count"),
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
                title="Approximate region size",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_raftstore_region_size_bucket", by_labels=["le"]
                        ),
                    ),
                ],
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
        ]
    )
    layout.row(
        [
            heatmap_panel(
                title="Region written bytes",
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_region_written_bytes_bucket", by_labels=["le"]
                        ),
                    ),
                ],
                yaxis=yaxis(format=UNITS.BYTES_IEC),
            ),
            graph_panel(
                title="Region average written bytes",
                description="The average rate of writing bytes to Regions per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate("tikv_region_written_bytes_sum"),
                            "/",
                            expr_sum_rate("tikv_region_written_bytes_count"),
                        ),
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
                targets=[
                    target(
                        expr=expr_sum_rate(
                            "tikv_region_written_keys_bucket", by_labels=["le"]
                        ),
                    ),
                ],
            ),
            graph_panel(
                title="Region average written keys",
                description="The average rate of written keys to Regions per TiKV instance",
                yaxes=yaxes(left_format=UNITS.BYTES_IEC),
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate("tikv_region_written_keys_sum"),
                            "/",
                            expr_sum_rate("tikv_region_written_keys_count"),
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
                            labels_selectors=['name=~"raftstore-.*"'],
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
                            by_labels=["name"],
                        ),
                        legend_format="{{name}}",
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
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_yatp_pool_schedule_wait_duration_sum",
                                by_labels=["name"],
                            ),
                            "/",
                            expr_sum_rate(
                                "tikv_yatp_pool_schedule_wait_duration_count",
                                by_labels=["name"],
                            ),
                        ),
                        legend_format="{{name}}",
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
                            labels_selectors=['metric="block_read_time"'],
                            by_labels=["req"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            labels_selectors=['metric="block_read_time"'],
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
                            labels_selectors=['metric="block_read_byte"'],
                            by_labels=["req"],
                        ),
                    ),
                    target(
                        expr=expr_sum_rate(
                            "tikv_coprocessor_rocksdb_perf",
                            labels_selectors=['metric="block_read_byte"'],
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
                title="Request batch input",
                description="The size of requests into request batch per TiKV instance",
                targets=[
                    target(
                        expr=expr_operator(
                            expr_sum_rate(
                                "tikv_server_request_batch_size_sum", by_labels=["type"]
                            ),
                            "/",
                            expr_sum_rate(
                                "tikv_server_request_batch_size_count",
                                by_labels=["type"],
                            ),
                        ),
                        legend_format="{{type}}-avg",
                    ),
                    target(
                        expr=expr_histogram_quantile(
                            0.99,
                            "tikv_server_request_batch_size",
                            by_labels=["type"],
                        ),
                        legend_format="{{type}}-99%",
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
        Server(),
        gRPC(),
    ],
).auto_panel_ids()
