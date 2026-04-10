import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import colorbar_ticks, get_database, logbins, plotly_chart, render_sidebar

st.set_page_config(
    page_title="Timing",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def total_execution_time_histogram(db, x_type="linear"):
    execution_times = pd.read_sql_query(
        """
        SELECT execution_time
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )
    if execution_times.empty:
        return None

    return histogram_with_kde(
        data=execution_times["execution_time"].tolist(),
        title="Tests by runtime",
        xaxis_title="Runtime (seconds)",
        yaxis_title="Test count",
        bin_size=0.01,
        x_type=x_type,
    )


def median_testcase_time_histogram(db, x_type="linear"):
    """Median per-test-case execution time, aggregated by test."""
    data = pd.read_sql_query(
        """
        SELECT median_execution_time
        FROM node_aggregate_metrics
        WHERE median_execution_time IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["median_execution_time"].tolist(),
        title="Median per-test-case execution time by test",
        xaxis_title="Median test case execution time (seconds)",
        yaxis_title="Test count",
        bin_size=0.001,
        x_type=x_type,
    )


def generation_time_percent_histogram(db):
    """% generation time by test."""
    data = pd.read_sql_query(
        """
        SELECT generation_percent
        FROM node_aggregate_metrics
        WHERE generation_percent IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["generation_percent"].tolist(),
        title="% generation time by test",
        xaxis_title="% generation time",
        yaxis_title="Test count",
        bin_size=1,
    )


def runtime_vs_generation_heatmap(db):
    """2D heatmap: runtime (x, log) vs % generation time (y)."""
    data = pd.read_sql_query(
        """
        SELECT rs.execution_time, nam.generation_percent
        FROM runtime_summary rs
        JOIN node_aggregate_metrics nam ON rs.node_id = nam.node_id
        WHERE rs.status IN ('passed', 'failed')
            AND rs.execution_time IS NOT NULL
            AND nam.generation_percent IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    x = data["generation_percent"]
    y = data["execution_time"].clip(lower=1e-6)

    xbins = np.linspace(0, 100, 51)
    ybins = logbins(y.min(), y.max())

    counts, xedges, yedges = np.histogram2d(x, y, bins=[xbins, ybins])
    z = np.log1p(counts.T)

    fig = go.Figure(
        go.Heatmap(
            x=xedges,
            y=yedges,
            z=z,
            colorscale="Blues",
            colorbar=colorbar_ticks(counts),
        )
    )
    fig.update_layout(
        title="% generation time vs runtime",
        xaxis_title="% generation time",
        xaxis_range=[0, 100],
        yaxis_title="Runtime (seconds)",
        yaxis_type="log",
        height=500,
    )
    return fig


def normalized_execution_time_curve(db):
    """Average execution time shape across all tests, normalized to 0-1 on both axes."""
    data = pd.read_sql_query(
        """
        WITH test_stats AS (
            SELECT
                node_id,
                MAX(test_case_number) as max_tc,
                AVG(json_extract(timing, '$."execute:test"')) as mean_time
            FROM runtime_test_case
            WHERE json_extract(timing, '$."execute:test"') IS NOT NULL
            GROUP BY node_id
            HAVING mean_time > 0
        ),
        binned AS (
            SELECT
                MIN(100, CAST(ROUND(CAST(tc.test_case_number AS FLOAT) / ts.max_tc * 100) AS INTEGER)) as bin,
                json_extract(tc.timing, '$."execute:test"') / ts.mean_time as normalized_time
            FROM runtime_test_case tc
            JOIN test_stats ts ON tc.node_id = ts.node_id
            WHERE json_extract(tc.timing, '$."execute:test"') IS NOT NULL
        )
        SELECT
            bin * 1.0 / 100 as percent_through,
            AVG(normalized_time) as mean_normalized_time,
            COUNT(*) as n
        FROM binned
        GROUP BY bin
        ORDER BY bin
        """,
        db._conn,
    )
    if data.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["percent_through"],
            y=data["mean_normalized_time"],
            mode="lines",
            name="Mean",
            line={"width": 2},
        )
    )
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        title="Normalized execution time shape",
        xaxis_title="% of test run progress",
        yaxis_title="Multiple of mean test execution time",
        height=400,
    )
    return fig


def generation_percent_over_run_curve(db):
    """Average generation % shape across all tests, normalized to 0-1 on x-axis.

    Reads precomputed per-node generation curves from node_aggregate_metrics
    and averages across nodes (equal weight per node).
    """
    data = pd.read_sql_query(
        """
        SELECT generation_curve
        FROM node_aggregate_metrics
        WHERE generation_curve IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    # Average per-node curves: each node contributes equally per bin
    bin_values: dict[int, list[float]] = {}
    for curve_json in data["generation_curve"]:
        curve = json.loads(curve_json)
        for bin_str, gen_pct in curve.items():
            bin_values.setdefault(int(bin_str), []).append(gen_pct)

    bins = sorted(bin_values.keys())
    agg = pd.DataFrame(
        {
            "percent_through": bins,
            "gen_percent": [sum(bin_values[b]) / len(bin_values[b]) for b in bins],
        }
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=agg["percent_through"],
            y=agg["gen_percent"],
            mode="lines",
            name="Mean",
            line={"width": 2},
        )
    )
    fig.update_layout(
        title="% generation time through the test",
        xaxis_title="% of test run progress",
        yaxis_title="% generation time",
        yaxis_range=[0, None],
        height=400,
    )
    return fig


def execution_time_cv_histogram(db):
    """Coefficient of variation (stddev/mean) of per-test-case execution time."""
    data = pd.read_sql_query(
        """
        SELECT execution_time_cv
        FROM node_aggregate_metrics
        WHERE execution_time_cv IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["execution_time_cv"].tolist(),
        title="Execution time coefficient of variation",
        xaxis_title="Coefficient of variation of test-case execution time",
        yaxis_title="Test count",
        bin_size=0.05,
    )


# =============================================================================
# Page
# =============================================================================


def main():
    render_sidebar()

    st.header("Timing")

    db = get_database()

    log_x_runtime = st.checkbox("Log x-axis", key="runtime_log_x", value=True)
    fig = total_execution_time_histogram(
        db, x_type="log" if log_x_runtime else "linear"
    )
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No execution time data available.")

    log_x_median = st.checkbox("Log x-axis", key="median_tc_log_x", value=True)
    fig = median_testcase_time_histogram(db, x_type="log" if log_x_median else "linear")
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No per-test-case timing data available.")

    fig = generation_time_percent_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No generation timing data available.")

    fig = runtime_vs_generation_heatmap(db)
    if fig:
        plotly_chart(fig, width="stretch")

    fig = normalized_execution_time_curve(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No normalized execution time data available.")

    fig = generation_percent_over_run_curve(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No generation % over run data available.")

    fig = execution_time_cv_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No execution time consistency data available.")


if __name__ == "__main__":
    main()
