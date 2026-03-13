import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import colorbar_ticks, get_database, plotly_chart, render_sidebar

st.set_page_config(
    page_title="Test Cases",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


# =============================================================================
# Data status
# =============================================================================


def overrun_percent_histogram(db):
    """% of overrun (data_status=0) test cases by test."""
    data = pd.read_sql_query(
        """
        SELECT percent_overrun
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["percent_overrun"].tolist(),
        title="% overrun test cases",
        xaxis_title="% overrun test cases",
        yaxis_title="Test count",
        bin_size=1,
    )


def invalid_percent_histogram(db):
    """% of invalid (data_status=1) test cases by test."""
    data = pd.read_sql_query(
        """
        SELECT percent_invalid
        FROM node_aggregate_metrics
        """,
        db._conn,
    )
    if data.empty:
        return None

    return histogram_with_kde(
        data=data["percent_invalid"].tolist(),
        title="% invalid test cases",
        xaxis_title="% invalid test cases",
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
        title="Median feature count per test case (tests with features only)",
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



def choices_size_vs_runtime_heatmap(db, *, log_x=False, log_y=False):
    """2D heatmap: choices_size (x) vs per-test-case execution time (y)."""
    data = pd.read_sql_query(
        """
        SELECT choices_size, json_extract(timing, '$."execute:test"') as execution_time
        FROM runtime_testcase
        WHERE json_extract(timing, '$."execute:test"') IS NOT NULL
        """,
        db._conn,
    )
    if data.empty:
        return None

    x = data["choices_size"].clip(lower=1) if log_x else data["choices_size"]
    y = data["execution_time"].clip(lower=1e-4) if log_y else data["execution_time"]

    xbins = np.logspace(np.log10(x.min()), np.log10(x.max()), 51) if log_x else 50
    ybins = np.logspace(np.log10(y.min()), np.log10(y.max()), 51) if log_y else 50

    counts, xedges, yedges = np.histogram2d(x, y, bins=[xbins, ybins])
    # log scale to avoid dominant cell washing out all others
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
        title="choices_size vs execution time",
        xaxis_title="choices_size",
        yaxis_title="Execution time (seconds)",
        xaxis_type="log" if log_x else None,
        yaxis_type="log" if log_y else None,
        height=500,
    )
    return fig


def choices_size_vs_generation_time_heatmap(db, *, log_x=False, log_y=False):
    """2D heatmap: choices_size (x) vs per-test-case generation time (y)."""
    data = pd.read_sql_query(
        """
        SELECT choices_size, timing
        FROM runtime_testcase
        """,
        db._conn,
    )
    if data.empty:
        return None

    gen_times = []
    for timing_json in data["timing"]:
        timing = json.loads(timing_json)
        gen_times.append(sum(v for k, v in timing.items() if k.startswith("generate:")))
    data["gen_time"] = gen_times

    data = data[data["gen_time"] > 0]
    if data.empty:
        return None

    x = data["choices_size"].clip(lower=1) if log_x else data["choices_size"]
    y = data["gen_time"].clip(lower=1e-6) if log_y else data["gen_time"]

    xbins = np.logspace(np.log10(x.min()), np.log10(x.max()), 51) if log_x else 50
    ybins = np.logspace(np.log10(y.min()), np.log10(y.max()), 51) if log_y else 50

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
        title="choices_size vs generation time",
        xaxis_title="choices_size",
        yaxis_title="Generation time (seconds)",
        xaxis_type="log" if log_x else None,
        yaxis_type="log" if log_y else None,
        height=500,
    )
    return fig


def choices_size_distribution_heatmap(db):
    """2D heatmap: median choices_size by test (x) vs individual choices_size (y)."""
    # Get per-node median and individual values in one query with sampling
    data = pd.read_sql_query(
        """
        SELECT tc.node_id, tc.choices_size
        FROM runtime_testcase tc
        """,
        db._conn,
    )
    if data.empty:
        return None

    # Compute median per node from the sample
    medians = data.groupby("node_id")["choices_size"].transform("median")
    data["median_cs"] = medians

    counts, xedges, yedges = np.histogram2d(
        data["median_cs"], data["choices_size"], bins=50
    )
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
        title="Median test choices_size vs test case choices_size",
        xaxis_title="Median test choices_size",
        yaxis_title="Test case choices_size",
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

    fig = overrun_percent_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No overrun data available.")

    fig = invalid_percent_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No filtering data available.")

    # --- Features ---
    st.subheader("Event / Note Usage")

    fig = median_feature_count_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")
    else:
        st.info("No tests with event()/note() features found.")

    # --- Choices size ---
    st.subheader("Test Case Size")

    fig = min_choices_size_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")

    fig = median_choices_size_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")

    fig = max_choices_size_histogram(db)
    if fig:
        plotly_chart(fig, width="stretch")

    col1, col2 = st.columns(2)
    log_x = (
        col1.radio(
            "x-axis", ["Linear", "Log"], index=1, horizontal=True, key="cs_rt_log_x"
        )
        == "Log"
    )
    log_y = (
        col2.radio(
            "y-axis", ["Linear", "Log"], index=1, horizontal=True, key="cs_rt_log_y"
        )
        == "Log"
    )
    fig = choices_size_vs_runtime_heatmap(db, log_x=log_x, log_y=log_y)
    if fig:
        plotly_chart(fig, width="stretch")

    col1, col2 = st.columns(2)
    log_x2 = (
        col1.radio(
            "x-axis", ["Linear", "Log"], index=1, horizontal=True, key="cs_gen_log_x"
        )
        == "Log"
    )
    log_y2 = (
        col2.radio(
            "y-axis", ["Linear", "Log"], index=1, horizontal=True, key="cs_gen_log_y"
        )
        == "Log"
    )
    fig = choices_size_vs_generation_time_heatmap(db, log_x=log_x2, log_y=log_y2)
    if fig:
        plotly_chart(fig, width="stretch")

    fig = choices_size_distribution_heatmap(db)
    if fig:
        plotly_chart(fig, width="stretch")


if __name__ == "__main__":
    main()
