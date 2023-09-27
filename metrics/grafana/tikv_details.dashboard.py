from typing import Optional, Union

import attr
from attr.validators import in_, instance_of
from grafanalib import formatunits as UNITS
from grafanalib.core import (
    GRAPH_TOOLTIP_MODE_SHARED_TOOLTIP,
    HIDE_VARIABLE,
    NULL_AS_NULL,
    SHOW,
    TIME_SERIES_TARGET_FORMAT,
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
    SeriesOverride,
    Stat,
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


@attr.s
class Expr(object):
    """
    A prometheus expression that matches the following grammar:

    expr ::= <aggr_op> (
                [aggr_param,]
                [func](
                    <metric name>
                    [{<label_selectors>,}]
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
    label_selectors: list[str] = attr.ib(default=[], validator=instance_of(list))
    by_labels: list[str] = attr.ib(default=[], validator=instance_of(list))
    default_label_selectors: list[str] = attr.ib(
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
        label_selectors = self.default_label_selectors + self.label_selectors
        assert all(
            ("=" in item or "~" in item) for item in label_selectors
        ), f"Not all items contain '=' or '~', invalid {self.label_selectors}"
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
        label_selectors: list[str] = [],
    ) -> "Expr":
        self.aggr_op = aggr_op
        self.aggr_param = aggr_param
        self.by_labels = by_labels
        self.label_selectors = label_selectors
        return self

    def function(
        self,
        func: str,
        label_selectors: list[str] = [],
        range_selector: str = "",
    ) -> "Expr":
        self.func = func
        self.label_selectors = label_selectors
        self.range_selector = range_selector
        return self

    def extra(
        self,
        extra_expr: Optional[str] = None,
        default_label_selectors: Optional[list[str]] = None,
    ) -> "Expr":
        if extra_expr is not None:
            self.extra_expr = extra_expr
        if default_label_selectors is not None:
            self.default_label_selectors = default_label_selectors
        return self


def expr_aggr(
    metric: str,
    aggr_op: str,
    aggr_param: str = "",
    label_selectors: list[str] = [],
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
        label_selectors=label_selectors,
    )
    return expr


def expr_sum(
    metric: str,
    label_selectors: list[str] = [],
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
        metric, "sum", label_selectors=label_selectors, by_labels=by_labels
    )


def expr_avg(
    metric: str,
    label_selectors: list[str] = [],
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
        metric, "avg", label_selectors=label_selectors, by_labels=by_labels
    )


def expr_max(
    metric: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the max of a metric.

    Example:

        max((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric, "max", label_selectors=label_selectors, by_labels=by_labels
    )


def expr_aggr_func(
    metric: str,
    aggr_op: str,
    func: str,
    aggr_param: str = "",
    label_selectors: list[str] = [],
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
        label_selectors=label_selectors,
        range_selector=range_selector,
    )
    return expr


def expr_sum_rate(
    metric: str,
    label_selectors: list[str] = [],
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
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
    )


def expr_sum_delta(
    metric: str,
    label_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
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
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
    )


def expr_simple(
    metric: str,
    label_selectors: list[str] = [],
) -> Expr:
    """
    Query an instant vector of a metric.

    Example:

    tikv_grpc_msg_duration_seconds_count
    {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
    """
    expr = Expr(metric=metric)
    expr.function("", label_selectors=label_selectors)
    return expr


def expr_operator(lhs: Union[Expr, str], operator: str, rhs: Union[Expr, str]) -> str:
    return f"""({lhs} {operator} {rhs})"""


def expr_histogram_quantile(
    quantile: float,
    metrics: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = [],
) -> Expr:
    """
    Query a quantile of a histogram metric.

    Example:

    histogram_quantile(0.99, sum(rate(
        tikv_grpc_msg_duration_seconds_bucket
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (le))
    """
    # sum(rate(metrics_bucket{label_selectors}[$__rate_interval])) by (le)
    assert not metrics.endswith(
        "_bucket"
    ), f"'{metrics}' should not specify '_bucket' suffix manually"
    by_labels = list(filter(lambda label: label != "le", by_labels))
    sum_rate_of_buckets = expr_sum_rate(
        metrics + "_bucket",
        label_selectors=label_selectors,
        by_labels=by_labels + ["le"],
    )
    # histogram_quantile({quantile}, {sum_rate_of_buckets})
    return expr_aggr(
        metric=f"{sum_rate_of_buckets}",
        aggr_op="histogram_quantile",
        aggr_param=f"{quantile}",
        label_selectors=[],
        by_labels=[],
    ).extra(
        # Do not attach default label selector again.
        default_label_selectors=[]
    )


def expr_histogram_avg(
    metrics: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> str:
    """
    Query the avg of a histogram metric.

    Example:

    sum(rate(
        tikv_grpc_msg_duration_seconds_sum
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance"}
        [$__rate_interval]
    )) / sum(rate(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance"}
        [$__rate_interval]
    ))
    """
    for suffix in ["_bucket", "_count", "_sum"]:
        assert not metrics.endswith(
            suffix
        ), f"'{metrics}' should not specify '{suffix}' suffix manually"

    return expr_operator(
        expr_sum_rate(
            metrics + "_sum",
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
        "/",
        expr_sum_rate(
            metrics + "_count",
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
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
    fill=1,
    fill_gradient=1,
    stack=False,
    thresholds: list[GraphThreshold] = [],
    series_overrides: list[SeriesOverride] = [],
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
        seriesOverrides=series_overrides,
        # Do not specify max max data points, let Grafana decide.
        maxDataPoints=None,
        extraJson=extraJson,
    )


def series_override(
    alias: str,
    bars: bool = False,
    lines: bool = True,
    yaxis: int = 1,
    fill: int = 1,
    zindex: int = 0,
    dashes: Optional[bool] = None,
    dash_length: Optional[int] = None,
    space_length: Optional[int] = None,
    transform_negative_y: bool = False,
) -> SeriesOverride:
    class SeriesOverridePatch(SeriesOverride):
        dashes_override: Optional[bool]
        dash_length_override: Optional[int]
        space_length_override: Optional[int]
        transform_negative_y: bool

        def __init__(self, *args, **kwargs) -> None:
            self.dashes_override = kwargs["dashes"]
            if self.dashes_override is None:
                del kwargs["dashes"]
            self.dash_length_override = kwargs["dashLength"]
            if self.dash_length_override is None:
                del kwargs["dashLength"]
            self.space_length_override = kwargs["spaceLength"]
            if self.space_length_override is None:
                del kwargs["spaceLength"]
            self.transform_negative_y = kwargs["transform_negative_y"]
            del kwargs["transform_negative_y"]
            super().__init__(*args, **kwargs)

        def to_json_data(self):
            data = super().to_json_data()
            # The default 'null' color makes it transparent, remove it.
            del data["color"]
            # The default 'null' makes it a transparent line, remove it.
            if self.dashes_override is None:
                del data["dashes"]
            if self.dash_length_override is None:
                del data["dashLength"]
            if self.space_length_override is None:
                del data["spaceLength"]
            # Add missing transform.
            if self.transform_negative_y:
                data["transform"] = "negative-Y"
            return data

    return SeriesOverridePatch(
        alias=alias,
        bars=bars,
        lines=lines,
        yaxis=yaxis,
        fill=fill,
        zindex=zindex,
        dashes=dashes,
        dashLength=dash_length,
        spaceLength=space_length,
        transform_negative_y=transform_negative_y,
    )


def yaxis(format: str, log_base=1) -> YAxis:
    assert format not in [
        UNITS.BYTES,
        UNITS.BITS,
        UNITS.KILO_BYTES,
        UNITS.MEGA_BYTES,
        UNITS.GIGA_BYTES,
        UNITS.TERA_BYTES,
        UNITS.PETA_BYTES,
        UNITS.BYTES_SEC,
        UNITS.KILO_BYTES_SEC,
        UNITS.MEGA_BYTES_SEC,
        UNITS.GIGA_BYTES_SEC,
        UNITS.TERA_BYTES_SEC,
        UNITS.PETA_BYTES_SEC,
    ], "Must not use SI bytes"
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
    metric: str,
    description=None,
    label_selectors: list[str] = [],
    yaxis=yaxis(UNITS.NO_FORMAT),
    tooltip=Tooltip(shared=True, valueType="individual"),
    color=heatmap_color(),
    data_source=DATASOURCE,
) -> Panel:
    assert metric.endswith(
        "_bucket"
    ), f"'{metric}' should be a histogram metric with '_bucket' suffix"
    t = target(
        expr=expr_sum_rate(
            metric, label_selectors=label_selectors, by_labels=["le"]
        )
    )
    # Make sure targets are in heatmap format.
    t.format = "heatmap"
    # Heatmap target legendFormat should be "{{le}}"
    t.legendFormat = "{{le}}"
    return Heatmap(
        title=title,
        dataSource=data_source,
        description=description,
        targets=[t],
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


def stat_panel(
    title: str,
    targets: list[Target],
    description=None,
    format=UNITS.NONE_FORMAT,
    graph_mode="none",
    data_source=DATASOURCE,
) -> Panel:
    for target in targets:
        # Make sure traget is in time_series format.
        target.format = TIME_SERIES_TARGET_FORMAT
    return Stat(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        format=format,
        graphMode=graph_mode,
        reduceCalc="lastNotNull",
    )


def graph_panel_histogram_quantiles(
    title: str,
    description: str,
    yaxes: YAxes,
    metric: str,
    label_selectors: list[str] = [],
    hide_count=False,
) -> Panel:
    """
    Return a graph panel that shows histogram quantiles of a metric.

    Targets:
        - 99.99% quantile
        - 99% quantile
        - avg
        - count
    """
    return graph_panel(
        title=title,
        description=description,
        yaxes=yaxes,
        targets=[
            target(
                expr=expr_histogram_quantile(
                    0.9999,
                    f"{metric}",
                    label_selectors=label_selectors,
                ),
                legend_format=r"99.99%",
            ),
            target(
                expr=expr_histogram_quantile(
                    0.99,
                    f"{metric}",
                    label_selectors=label_selectors,
                ),
                legend_format=r"99%",
            ),
            target(
                expr=expr_histogram_avg(
                    metric,
                    label_selectors=label_selectors,
                    by_labels=[],  # override default by instance.
                ),
                legend_format="avg",
            ),
            target(
                expr=expr_sum_rate(
                    f"{metric}_count",
                    label_selectors=label_selectors,
                    by_labels=[],  # override default by instance.
                ),
                legend_format="count",
                hide=hide_count,
            ),
        ],
        series_overrides=[
            series_override(
                alias="count",
                fill=2,
                yaxis=2,
                zindex=-3,
                dashes=True,
                dash_length=1,
                space_length=1,
                transform_negative_y=True,
            ),
            series_override(
                alias="avg",
                fill=7,
            ),
        ],
    )


def heatmap_panel_graph_panel_histogram_quantile_pairs(
    heatmap_title: str,
    heatmap_description: str,
    graph_title: str,
    graph_description: str,
    quantile: float,
    yaxis_format: str,
    metric: str,
    label_selectors=[],
) -> list[Panel]:
    return [
        heatmap_panel(
            title=heatmap_title,
            description=heatmap_description,
            yaxis=yaxis(format=yaxis_format),
            metric=f"{metric}_bucket",
            label_selectors=label_selectors,
        ),
        graph_panel(
            title=graph_title,
            description=graph_description,
            yaxes=yaxes(left_format=yaxis_format),
            targets=[
                target(
                    expr=expr_histogram_quantile(
                        quantile,
                        f"{metric}",
                        label_selectors=label_selectors,
                        by_labels=["instance"],
                    ),
                    legend_format="{{instance}}",
                ),
            ],
        ),
    ]


#### Utilities Function End ####

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
                        expr=expr_histogram_avg(
                            "tikv_yatp_pool_schedule_wait_duration",
                            by_labels=["name"],
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
                title="Storage ReadPool CPU",
                description="The CPU utilization of readpool",
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
        ]
    )
    layout.row(
        [
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
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Coprocessor CPU",
                description="The CPU utilization of coprocessor",
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
                title="Background Worker CPU",
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
                title="Raftlog fetch Worker CPU",
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
                title="TSO Worker CPU",
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
    return layout.row_panel


def TTL() -> RowPanel:
    layout = Layout(title="TTL")
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
            )
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
            quantile=0.99,
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
            quantile=0.99,
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_store_write_loop_duration_seconds",
        )
    )
    layout.row(
        heatmap_panel_graph_panel_histogram_quantile_pairs(
            heatmap_title="Append log duration",
            heatmap_description="The time consumed when Raft appends log",
            graph_title="99% Commit log duration per server",
            graph_description="The time consumed when Raft commits log on each TiKV instance",
            quantile=0.99,
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
            quantile=0.99,
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
            quantile=0.99,
            yaxis_format=UNITS.SECONDS,
            metric="tikv_raftstore_apply_log_duration_seconds",
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
            quantile=0.99,
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
            quantile=0.99,
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
            quantile=0.99,
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
            # graph_panel()
        ]
    )
    return layout.row_panel


def RaftMessage() -> RowPanel:
    layout = Layout(title="Raft Message")
    layout.row(
        [
            # graph_panel()
        ]
    )
    return layout.row_panel


def RaftAdmin() -> RowPanel:
    layout = Layout(title="Raft Admin")
    layout.row(
        [
            # graph_panel()
        ]
    )
    return layout.row_panel


def RaftLog() -> RowPanel:
    layout = Layout(title="Raft Log")
    layout.row(
        [
            # graph_panel()
        ]
    )
    return layout.row_panel


def LocalReader() -> RowPanel:
    layout = Layout(title="Local Reader")
    layout.row(
        [
            # graph_panel()
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
    templating=Templates(),
    panels=[
        Duration(),
        Cluster(),
        Errors(),
        Server(),
        gRPC(),
        ThreadCPU(),
        TTL(),
        PD(),
        IOBreakdown(),
        RaftWaterfall(),
        RaftIO(),
        RaftPropose(),
        RaftProcess(),
        RaftMessage(),
        RaftAdmin(),
        RaftLog(),
        LocalReader(),
    ],
).auto_panel_ids()
