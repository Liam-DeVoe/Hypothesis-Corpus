import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import get_database, render_sidebar

st.set_page_config(
    page_title="Test Cases",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


# =============================================================================
# Data status
# =============================================================================


def overrun_pct_histogram(db):
    """% of overrun (data_status=0) test cases per test."""
    data = pd.read_sql_query(
        """
        SELECT pct_overrun
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["pct_overrun"].tolist(),
        title="% overrun test cases per test",
        xaxis_title="% of test cases that overran",
        yaxis_title="Test count",
        bin_size=1,
    )


def filtered_pct_histogram(db):
    """% of filtered (data_status=1) test cases per test."""
    data = pd.read_sql_query(
        """
        SELECT pct_filtered
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["pct_filtered"].tolist(),
        title="% filtered test cases per test",
        xaxis_title="% of test cases filtered (assume/filter)",
        yaxis_title="Test count",
        bin_size=1,
    )


# =============================================================================
# Event / note usage
# =============================================================================


def median_feature_count_histogram(db):
    """Median number of features per test case, by test."""
    data = pd.read_sql_query(
        """
        SELECT median_feature_count
        FROM node_aggregate_metrics
        WHERE median_feature_count > 0
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["median_feature_count"].tolist(),
        title="Median event()/note() count per test case (tests with features only)",
        xaxis_title="Median feature count per test case",
        yaxis_title="Test count",
        bin_size=1,
    )


# =============================================================================
# Choices size
# =============================================================================


def min_choices_size_histogram(db):
    data = pd.read_sql_query(
        """
        SELECT min_choices_size
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["min_choices_size"].tolist(),
        title="Min choices_size by test",
        xaxis_title="Min choices_size",
        yaxis_title="Test count",
        bin_size=5,
    )


def median_choices_size_histogram(db):
    data = pd.read_sql_query(
        """
        SELECT median_choices_size
        FROM node_aggregate_metrics
        WHERE median_choices_size IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["median_choices_size"].tolist(),
        title="Median choices_size by test",
        xaxis_title="Median choices_size",
        yaxis_title="Test count",
        bin_size=5,
    )


def max_choices_size_histogram(db):
    data = pd.read_sql_query(
        """
        SELECT max_choices_size
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["max_choices_size"].tolist(),
        title="Max choices_size by test",
        xaxis_title="Max choices_size",
        yaxis_title="Test count",
        bin_size=5,
    )


def choices_size_vs_runtime_heatmap(db):
    """2D heatmap: choices_size (x) vs per-test-case execution time (y)."""
    data = pd.read_sql_query(
        """
        SELECT choices_size, json_extract(timing, '$."execute:test"') as exec_time
        FROM runtime_testcase
        WHERE json_extract(timing, '$."execute:test"') IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 50000
        """,
        db._conn,
    )
    if data.empty:
        return None

    fig = go.Figure(
        go.Histogram2d(
            x=data["choices_size"],
            y=data["exec_time"],
            colorscale="Blues",
            nbinsx=50,
            nbinsy=50,
        )
    )
    fig.update_layout(
        title="choices_size vs test case execution time",
        xaxis_title="choices_size",
        yaxis_title="Execution time (seconds)",
        height=500,
    )
    return fig


def choices_size_distribution_heatmap(db):
    """2D heatmap: median choices_size per test (x) vs individual choices_size (y)."""
    # Get per-node median and individual values in one query with sampling
    data = pd.read_sql_query(
        """
        SELECT tc.node_id, tc.choices_size
        FROM runtime_testcase tc
        ORDER BY RANDOM()
        LIMIT 50000
        """,
        db._conn,
    )
    if data.empty:
        return None

    # Compute median per node from the sample
    medians = data.groupby("node_id")["choices_size"].transform("median")
    data["median_cs"] = medians

    fig = go.Figure(
        go.Histogram2d(
            x=data["median_cs"],
            y=data["choices_size"],
            colorscale="Blues",
            nbinsx=50,
            nbinsy=50,
        )
    )
    fig.update_layout(
        title="Median choices_size vs individual test case choices_size",
        xaxis_title="Median choices_size of test",
        yaxis_title="Individual test case choices_size",
        height=500,
    )
    return fig


# =============================================================================
# Page
# =============================================================================


def main():
    render_sidebar()

    st.header("Test Cases")

    db = get_database()

    # --- Data status ---
    st.subheader("Data Status")

    fig = overrun_pct_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No overrun data available.")

    fig = filtered_pct_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No filtering data available.")

    # --- Features ---
    st.subheader("Event / Note Usage")

    fig = median_feature_count_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No tests with event()/note() features found.")

    # --- Choices size ---
    st.subheader("Test Case Size")

    fig = min_choices_size_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    fig = median_choices_size_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    fig = max_choices_size_histogram(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    fig = choices_size_vs_runtime_heatmap(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    fig = choices_size_distribution_heatmap(db)
    if fig:
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
