
import typing
from grafanalib.core import (
    Dashboard, TimeSeries, Template, HIDE_VARIABLE, SHOW,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR, Target, GridPos,
    DataSourceInput, Templating, RowPanel,Panel)
from grafanalib import formatunits as UNITS


DATASOURCE_INPUT= DataSourceInput(
        name="DS_TEST-CLUSTER",
        label="test-cluster",
        pluginId="prometheus",
        pluginName="Prometheus",
    )
DATASOURCE = "${{{}}}".format(DATASOURCE_INPUT.name)

def template(name, query, dataSource, hide, regex=None):
    return Template(
        dataSource=dataSource,
        hide=hide,
        includeAll=False,
        label=name,
        multi=False,
        name=name,
        query=query,
        refresh=2,
        sort=1,
        type="query",
        useTags = False,
        regex=regex,
    )

# Rows are always 24 "units" wide.
ROW_WIDTH = 24

# Evenly scales panels width.
def row(panels: typing.List[Panel]) -> typing.List[Panel]:
    count = len(panels)
    if count == 0:
        return
    width = ROW_WIDTH // count
    remain = ROW_WIDTH % count
    x=0
    for panel in panels:
        panel.gridPos.w = width
        panel.gridPos.x = x
        x += width
    panels[-1].gridPos.w += remain
    return panels

def row_panel(title, panels, collapsed=True) -> RowPanel:
    return RowPanel(
        title=title,
        gridPos=GridPos(h=1, w=ROW_WIDTH, x=0, y=0),
        collapsed=collapsed,
        panels=panels)

def target(expr, legendFormat, hide=False) -> Target:
    return Target(
                    expr=expr,
                    hide=hide,
                    legendFormat=legendFormat,
                )

def timeseries_panel(
        title, targets,
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
        dataSource=DATASOURCE) -> RowPanel:
    return TimeSeries(
            title=title,
            dataSource=dataSource,
            description=description,
            targets=targets,
            gridPos=GridPos(h=6, w=0, x=0, y=0),
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

#### Metrics Definition Start ####

def Templats() -> Templating:
    return Templating([
        template(
            name="k8s_cluster",
            query="label_values(tikv_engine_block_cache_size_bytes, k8s_cluster)",
            dataSource=DATASOURCE,
            hide=HIDE_VARIABLE,
        ),
        template(
            name="tidb_cluster",
            query="label_values(tikv_engine_block_cache_size_bytes{k8s_cluster=\"$k8s_cluster\"}, tidb_cluster)",
            dataSource=DATASOURCE,
            hide=HIDE_VARIABLE,
        ),
        template(
            name="db",
            query="label_values(tikv_engine_block_cache_size_bytes{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\"}, db)",
            dataSource=DATASOURCE,
            hide=SHOW,
        ),
        template(
            name="command",
            query="label_values(tikv_storage_command_total{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\"}, type)",
            dataSource=DATASOURCE,
            hide=SHOW,
            regex="/type=\"([^\"]+)\"/",
        ),
        template(
            name="instance",
            query="label_values(tikv_engine_size_bytes{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\"}, instance)",
            dataSource=DATASOURCE,
            hide=SHOW,
        ),
        template(
            name="titan_db",
            query="label_values(tikv_engine_titandb_num_live_blob_file{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\"}, db)",
            dataSource=DATASOURCE,
            hide=HIDE_VARIABLE,
        ),
    ])

def Duration() -> RowPanel:
    return row_panel(
        title="Duration",
        panels=row([
            timeseries_panel(
                title="Write Pipeline Duration",
                targets=[
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_raftstore_append_log_duration_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Write Raft Log .99",
                    ),
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_raftstore_request_wait_time_duration_secs_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Propose Wait .99",
                    ),
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_raftstore_apply_wait_time_duration_secs_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Apply Wait .99",
                    ),
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_raftstore_commit_log_duration_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Replicate Raft Log .99",
                    ),
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_raftstore_apply_log_duration_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Apply Duration .99",
                    ),
                ]
            ),
            timeseries_panel(
                title="Cop Read Duration",
                description="Read Duration Composition",
                targets=[
                    target(
                        expr="histogram_quantile(0.99, sum(delta(tikv_storage_engine_async_request_duration_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\", type=\"snapshot\"}[1m])) by (le))",
                        legendFormat="Get Snapshot .99",
                    ),
                    target(
                        expr="histogram_quantile(0.99, sum(rate(tikv_coprocessor_request_wait_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Cop Wait .99",
                    ),
                    target(
                        expr="histogram_quantile(0.95, sum(rate(tikv_coprocessor_request_handle_seconds_bucket{k8s_cluster=\"$k8s_cluster\", tidb_cluster=\"$tidb_cluster\", instance=~\"$instance\"}[1m])) by (le))",
                        legendFormat="Cop Handle .99",
                    ),
                ]
            ),
        ]),
    )

#### Metrics Definition End ####

dashboard = Dashboard(
    title="Test-Cluster-TiKV-Detailsaa",
    uid="RDVQiEzZzaa",
    timezone="browser",
    refresh="1m",
    inputs=[DATASOURCE_INPUT],
    editable=True,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,

    templating=Templats(),
    panels=[
        Duration(),
    ],
).auto_panel_ids()
