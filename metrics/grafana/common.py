from __future__ import annotations

from typing import Optional, Union

import attr
from attr.validators import in_, instance_of
from grafanalib import formatunits as UNITS
from grafanalib.core import (
    NULL_AS_ZERO,
    TIME_SERIES_TARGET_FORMAT,
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
    StatValueMappings,
    Target,
    Template,
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
ADDITIONAL_GROUPBY = "$additional_groupby"


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
    skip_default_instance: bool = attr.ib(default=False, validator=instance_of(bool))
    extra_expr: str = attr.ib(default="", validator=instance_of(str))

    def __str__(self) -> str:
        aggr_opeator = self.aggr_op if self.aggr_op else ""
        aggr_param = self.aggr_param + "," if self.aggr_param else ""
        by_clause = (
            "by ({})".format(", ".join(self.by_labels)) if self.by_labels else ""
        )
        func = self.func if self.func else ""
        label_selectors = self.default_label_selectors + self.label_selectors
        if self.skip_default_instance:
            # Remove instance=~"$instance"
            label_selectors = [l for l in label_selectors if "$instance" not in l]
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

    def skip_default_instance_selector(self) -> "Expr":
        self.skip_default_instance = True
        return self

    def append_by_labels(self, label: str) -> "Expr":
        self.by_labels.append(label)
        return self


class OpExpr:
    lhs: Union[Expr, OpExpr, str]
    op: str
    rhs: Union[Expr, OpExpr, str]

    def __init__(
        self, lhs: Union[Expr, OpExpr, str], op: str, rhs: Union[Expr, OpExpr, str]
    ):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __str__(self) -> str:
        return f"""({self.lhs} {self.op} {self.rhs})"""

    def __repr__(self) -> str:
        return self.__str__()

    def append_by_labels(self, label: str) -> "OpExpr":
        if not isinstance(self.lhs, str):
            self.lhs.append_by_labels(label)
        if not isinstance(self.rhs, str):
            self.rhs.append_by_labels(label)
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


def expr_min(
    metric: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the min of a metric.

    Example:

        min((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric, "min", label_selectors=label_selectors, by_labels=by_labels
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


def expr_sum_increase(
    metric: str,
    label_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of increase of a metric.

    Example:

    sum(increase(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="increase",
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
    )


def expr_sum_aggr_over_time(
    metric: str,
    aggr: str,
    range_selector: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of average value of all points in the specified interval of a metric.

    Example:

    sum(avg_over_time(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [1m]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func=f"{aggr}_over_time",
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
    )


def expr_max_rate(
    metric: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the max of rate of a metric.

    Example:

    max(rate(
        tikv_thread_voluntary_context_switches
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (name)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    return expr_aggr_func(
        metric=metric,
        aggr_op="max",
        func="rate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
    )


def expr_count_rate(
    metric: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the count of rate of a metric.

    Example:

    count(rate(
        tikv_thread_cpu_seconds_total
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",name=~"sst_.*"}
        [$__rate_interval]
    )) by (instance)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    return expr_aggr_func(
        metric=metric,
        aggr_op="count",
        func="rate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
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


def expr_operator(
    lhs: Union[Expr, OpExpr, str], operator: str, rhs: Union[Expr, OpExpr, str]
) -> str:
    return OpExpr(lhs, operator, rhs)


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


def expr_topk(
    k: int,
    metrics: str,
) -> Expr:
    """
    Query topk of a metric.

    Example:

    topk(20, tikv_thread_voluntary_context_switches)
    """
    # topk({k}, {metric})
    return expr_aggr(
        metric=metrics,
        aggr_op="topk",
        aggr_param=f"{k}",
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
) -> OpExpr:
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

    return OpExpr(
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
    expr: Union[Expr, OpExpr, str],
    legend_format: Optional[str] = None,
    hide=False,
    data_source=DATASOURCE,
    interval_factor=1,  # Prefer "high" resolution
    additional_groupby=False,
) -> Target:
    if isinstance(expr, Expr):
        if legend_format is None and expr.by_labels:
            legend_format = "-".join(
                map(lambda x: "{{" + f"{x}" + "}}", expr.by_labels)
            )
        if additional_groupby:
            expr.append_by_labels(ADDITIONAL_GROUPBY)
            legend_format += " {{" + ADDITIONAL_GROUPBY + "}}"
    elif isinstance(expr, OpExpr):
        assert legend_format is not None, "legend_format must be specified"
        if additional_groupby:
            expr.append_by_labels(ADDITIONAL_GROUPBY)
            legend_format += " {{" + ADDITIONAL_GROUPBY + "}}"

    return Target(
        expr=f"{expr}",
        hide=hide,
        legendFormat=legend_format,
        intervalFactor=interval_factor,
        datasource=data_source,
    )


def template(
    name,
    type,
    query,
    data_source,
    hide,
    regex=None,
    multi=False,
    include_all=False,
    all_value=None,
) -> Template:
    return Template(
        dataSource=data_source,
        hide=hide,
        label=name,
        multi=multi,
        name=name,
        query=query,
        refresh=2,
        sort=1,
        type=type,
        useTags=False,
        regex=regex,
        includeAll=include_all,
        allValue=all_value,
    )


class Layout:
    # Rows are always 24 "units" wide.
    ROW_WIDTH = 24
    PANEL_HEIGHT = 7
    row_panel: RowPanel
    current_row_y_pos: int
    current_row_x_pos: int

    def __init__(self, title, collapsed=True, repeat: Optional[str] = None) -> None:
        extraJson = None
        if repeat:
            extraJson = {"repeat": repeat}
            title = f"{title} - ${repeat}"
        self.current_row_y_pos = 0
        self.current_row_x_pos = 0
        self.row_panel = RowPanel(
            title=title,
            gridPos=GridPos(h=self.PANEL_HEIGHT, w=self.ROW_WIDTH, x=0, y=0),
            collapsed=collapsed,
            extraJson=extraJson,
        )

    def row(self, panels: list[Panel], width: int = ROW_WIDTH):
        """Start a new row and evenly scales panels width"""
        count = len(panels)
        if count == 0:
            return panels
        width = width // count
        remain = self.ROW_WIDTH % count
        x = self.current_row_x_pos % self.ROW_WIDTH
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
        self.current_row_x_pos = x

    def half_row(self, panels: list[Panel]):
        self.row(panels, self.ROW_WIDTH // 2)


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
    yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
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
    null_point_mode=NULL_AS_ZERO,
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
        # Make sure target is in time_series format.
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
        nullPointMode=null_point_mode,
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
    decimals=1,
    data_source=DATASOURCE,
) -> Panel:
    assert metric.endswith(
        "_bucket"
    ), f"'{metric}' should be a histogram metric with '_bucket' suffix"
    t = target(
        expr=expr_sum_rate(metric, label_selectors=label_selectors, by_labels=["le"]),
    )
    # Make sure targets are in heatmap format.
    t.format = "heatmap"
    # Heatmap target legendFormat should be "{{le}}"
    t.legendFormat = "{{le}}"
    # Overrides yaxis decimal places.
    yaxis.decimals = decimals
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
        # Limit data points, because too many data points slows browser when
        # the resolution is too high.
        # See: https://grafana.com/blog/2020/06/23/how-to-visualize-prometheus-histograms-in-grafana/
        maxDataPoints=512,
        # Fix grafana heatmap migration panic if options is null.
        # See: https://github.com/grafana/grafana/blob/v9.5.14/public/app/plugins/panel/heatmap/migrations.ts#L17
        options={},
    )


def stat_panel(
    title: str,
    targets: list[Target],
    description=None,
    format=UNITS.NONE_FORMAT,
    graph_mode="none",
    decimals: Optional[int] = None,
    mappings: Optional[StatValueMappings] = None,
    text_mode: str = "auto",
    data_source=DATASOURCE,
) -> Panel:
    for target in targets:
        # Make sure target is in time_series format.
        target.format = TIME_SERIES_TARGET_FORMAT
    return Stat(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        format=format,
        graphMode=graph_mode,
        reduceCalc="lastNotNull",
        decimals=decimals,
        mappings=mappings,
        textMode=text_mode,
    )


def graph_panel_histogram_quantiles(
    title: str,
    description: str,
    yaxes: YAxes,
    metric: str,
    label_selectors: list[str] = [],
    by_labels: list[str] = [],
    hide_p9999=False,
    hide_avg=False,
    hide_count=False,
    additional_groupby=True,
) -> Panel:
    """
    Return a graph panel that shows histogram quantiles of a metric.

    Targets:
        - 99.99% quantile
        - 99% quantile
        - avg
        - count
    """

    def legend(prefix, labels):
        if not labels:
            return prefix
        else:
            return "-".join([prefix] + ["{{%s}}" % lb for lb in labels])

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
                    by_labels=by_labels,
                ),
                legend_format=legend("99.99%", by_labels),
                hide=hide_p9999,
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_histogram_quantile(
                    0.99,
                    f"{metric}",
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("99%", by_labels),
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_histogram_avg(
                    metric,
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("avg", by_labels),
                hide=hide_avg,
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_sum_rate(
                    f"{metric}_count",
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("count", by_labels),
                hide=hide_count,
                additional_groupby=additional_groupby,
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
    yaxis_format: str,
    metric: str,
    label_selectors=[],
    graph_by_labels=[],
    graph_hides: list[str] = ["count"],
) -> list[Panel]:
    hide_count = False
    hide_avg = False
    for hide in graph_hides:
        if hide == "count":
            hide_count = True
        elif hide == "avg":
            hide_avg = True

    return [
        heatmap_panel(
            title=heatmap_title,
            description=heatmap_description,
            yaxis=yaxis(format=yaxis_format),
            metric=f"{metric}_bucket",
            label_selectors=label_selectors,
        ),
        graph_panel_histogram_quantiles(
            title=graph_title,
            description=graph_description,
            metric=f"{metric}",
            yaxes=yaxes(left_format=yaxis_format),
            label_selectors=label_selectors,
            by_labels=graph_by_labels,
            hide_count=hide_count,
            hide_avg=hide_avg,
        ),
    ]
