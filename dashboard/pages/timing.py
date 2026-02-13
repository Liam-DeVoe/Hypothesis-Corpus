import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import get_database, render_sidebar

st.set_page_config(
    page_title="Timing",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def total_execution_time_histogram(db):
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
        title="Tests by total execution time",
        xaxis_title="Execution time (seconds)",
        yaxis_title="Test count",
        bin_size=0.01,
    )


def median_testcase_time_histogram(db):
    """Median per-test-case execution time, aggregated per test."""
    data = pd.read_sql_query(
        """
        SELECT median_exec_time
        FROM node_aggregate_metrics
        WHERE median_exec_time IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["median_exec_time"].tolist(),
        title="Median per-test-case execution time by test",
        xaxis_title="Median test case execution time (seconds)",
        yaxis_title="Test count",
        bin_size=0.001,
    )


def generation_time_pct_histogram(db):
    """% of time spent in generation vs execution, per test."""
    data = pd.read_sql_query(
        """
        SELECT median_generation_pct
        FROM node_aggregate_metrics
        WHERE median_generation_pct IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["median_generation_pct"].tolist(),
        title="% of time spent in generation (median per test)",
        xaxis_title="% time in generation",
        yaxis_title="Test count",
        bin_size=1,
    )


def runtime_vs_generation_heatmap(db):
    """2D heatmap: total runtime (x) vs % generation time (y)."""
    data = pd.read_sql_query(
        """
        SELECT rs.execution_time, nam.median_generation_pct
        FROM runtime_summary rs
        JOIN node_aggregate_metrics nam ON rs.node_id = nam.node_id
        WHERE rs.status IN ('passed', 'failed')
            AND rs.execution_time IS NOT NULL
            AND nam.median_generation_pct IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    fig = go.Figure(
        go.Histogram2d(
            x=data["execution_time"],
            y=data["median_generation_pct"],
            colorscale="Blues",
            nbinsx=50,
            nbinsy=50,
        )
    )
    fig.update_layout(
        title="Total runtime vs % generation time",
        xaxis_title="Total execution time (seconds)",
        yaxis_title="% time in generation",
        yaxis_range=[0, 100],
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
                COUNT(*) as tc_count,
                MAX(testcase_number) as max_tc,
                AVG(json_extract(timing, '$."execute:test"')) as mean_time
            FROM runtime_testcase
            WHERE json_extract(timing, '$."execute:test"') IS NOT NULL
            GROUP BY node_id
            HAVING tc_count >= 50 AND mean_time > 0
        ),
        binned AS (
            SELECT
                MIN(100, CAST(ROUND(CAST(tc.testcase_number AS FLOAT) / ts.max_tc * 100) AS INTEGER)) as bin,
                json_extract(tc.timing, '$."execute:test"') / ts.mean_time as normalized_time
            FROM runtime_testcase tc
            JOIN test_stats ts ON tc.node_id = ts.node_id
            WHERE json_extract(tc.timing, '$."execute:test"') IS NOT NULL
        )
        SELECT
            bin * 1.0 / 100 as pct_through,
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
            x=data["pct_through"],
            y=data["mean_normalized_time"],
            mode="lines",
            name="Mean",
            line={"width": 2},
        )
    )
    fig.add_hline(y=1.0, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(
        title="Execution time shape over the run (normalized)",
        xaxis_title="% through the run",
        yaxis_title="Relative execution time (1.0 = test mean)",
        height=400,
    )
    return fig


def execution_time_cv_histogram(db):
    """Coefficient of variation (stddev/mean) of per-test-case execution time."""
    data = pd.read_sql_query(
        """
        SELECT exec_time_cv
        FROM node_aggregate_metrics
        WHERE exec_time_cv IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["exec_time_cv"].tolist(),
        title="Execution time consistency (coefficient of variation)",
        xaxis_title="CV (stddev / mean) of per-test-case execution time",
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

    fig = total_execution_time_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No execution time data available.")

    fig = median_testcase_time_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No per-test-case timing data available.")

    fig = generation_time_pct_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No generation timing data available.")

    fig = runtime_vs_generation_heatmap(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    fig = normalized_execution_time_curve(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No normalized execution time data available.")

    fig = execution_time_cv_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No execution time consistency data available.")


if __name__ == "__main__":
    main()
