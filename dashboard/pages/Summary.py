import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from scipy import stats

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.utils import get_database, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Summary",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)
db = get_database()


def histogram_with_kde(
    data: list,
    title: str,
    xaxis_title: str,
    yaxis_title: str,
    bin_size: float = 1,
    height: int = 400,
) -> go.Figure:
    """Create a histogram with KDE overlay.

    Args:
        data: List of numerical values to plot
        title: Chart title
        xaxis_title: X-axis label
        yaxis_title: Y-axis label
        bin_size: Size of histogram bins (default: 1)
        height: Chart height in pixels (default: 400)

    Returns:
        Plotly figure with histogram and KDE overlay
    """
    # Create figure with histogram
    fig = go.Figure()

    # Add histogram
    fig.add_trace(
        go.Histogram(
            x=data,
            name="Count",
            xbins={"size": bin_size},
            marker_color="steelblue",
        )
    )

    # Add KDE overlay
    kde = stats.gaussian_kde(data)
    x_range = np.linspace(min(data), max(data), 200)
    kde_values = kde(x_range)
    # Scale KDE to match histogram counts
    kde_scaled = kde_values * len(data) * bin_size

    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=kde_scaled,
            mode="lines",
            name="KDE",
            line={"color": "rgba(255, 127, 14, 0.8)", "width": 1.5},
        )
    )

    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        height=height,
        showlegend=True,
    )

    return fig


def hypothesis_test_count_histogram():
    repo_counts = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            json_array_length(node_ids) as node_count
        FROM core_repository
        WHERE json_array_length(node_ids) > 0
        """,
        db._conn,
    )

    if repo_counts.empty:
        return None

    return histogram_with_kde(
        data=repo_counts["node_count"].tolist(),
        title="Hypothesis test count by repository",
        xaxis_title="# of Hypothesis tests",
        yaxis_title="Repository count",
        bin_size=1,
    )


def unique_test_count_histogram():
    repo_counts = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            node_ids
        FROM core_repository
        WHERE json_array_length(node_ids) > 0
        """,
        db._conn,
    )

    if repo_counts.empty:
        return None

    # Count unique test functions (strip parametrization)
    unique_counts = []
    for _, row in repo_counts.iterrows():
        node_ids = json.loads(row["node_ids"])
        # Strip parametrization by removing everything after the first '[' if present
        unique_tests = set()
        for node_id in node_ids:
            # Remove parametrization: "test.py::test_foo[param]" -> "test.py::test_foo"
            base_test = node_id.split("[")[0] if "[" in node_id else node_id
            unique_tests.add(base_test)
        unique_counts.append(len(unique_tests))

    return histogram_with_kde(
        data=unique_counts,
        title="Hypothesis test count by repository (grouping @pytest.mark.parametrize)",
        xaxis_title="# of Hypothesis tests (grouping @pytest.mark.parametrize)",
        yaxis_title="Repository count",
        bin_size=1,
    )


def hypothesis_percentage_histogram():
    repo_data = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            json_array_length(node_ids) as node_count,
            json_array_length(other_node_ids) as other_node_count,
            CAST(json_array_length(node_ids) AS FLOAT) /
                (json_array_length(node_ids) + json_array_length(other_node_ids)) * 100
                as hypothesis_percentage
        FROM core_repository
        WHERE json_array_length(node_ids) > 0 OR json_array_length(other_node_ids) > 0
        """,
        db._conn,
    )

    if repo_data.empty:
        return None

    return histogram_with_kde(
        data=repo_data["hypothesis_percentage"].tolist(),
        title="% of repository tests that are Hypothesis tests",
        xaxis_title="% of repository tests that are Hypothesis tests",
        yaxis_title="Repository count",
        bin_size=0.5,
    )


def timing_histogram():
    execution_times = pd.read_sql_query(
        """
        SELECT execution_time
        FROM runtime_summary
        WHERE execution_time IS NOT NULL
        """,
        db._conn,
    )

    if execution_times.empty or len(execution_times) == 0:
        return None

    return histogram_with_kde(
        data=execution_times["execution_time"].tolist(),
        title="Nodes by execution time",
        xaxis_title="Execution time (seconds)",
        yaxis_title="Node count",
        bin_size=1,
    )


def execution_frequency_histogram():
    db = get_database()

    all_line_execution_data = pd.read_sql_query(
        """
        SELECT
            rs.line_execution_counts,
            rs.count_test_cases
        FROM runtime_summary rs
        WHERE rs.line_execution_counts IS NOT NULL
        """,
        db._conn,
    )

    if all_line_execution_data.empty:
        return None

    # Aggregate all frequencies across all nodes
    all_frequencies = []

    for _, row in all_line_execution_data.iterrows():
        line_counts_dict = json.loads(row["line_execution_counts"])

        if not line_counts_dict:
            continue

        # Flatten nested dict structure: {"file": {"line": count}} -> [counts]
        all_counts = []
        for file_counts in line_counts_dict.values():
            all_counts.extend(file_counts.values())

        if not all_counts:
            continue

        total_test_cases = row["count_test_cases"]
        # Calculate execution frequencies as percentages
        frequencies = [count / total_test_cases * 100 for count in all_counts]
        all_frequencies.extend(frequencies)

    if not all_frequencies:
        return None

    return histogram_with_kde(
        data=all_frequencies,
        title="Line Execution Frequency Distribution",
        xaxis_title="Line execution frequency (% of total test cases)",
        yaxis_title="Line count",
        bin_size=1,
    )


def main():
    """Summary page with key research findings."""
    # Sidebar
    render_sidebar()

    st.header("Summary")

    fig = hypothesis_test_count_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Hypothesis test data available.")

    fig = unique_test_count_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No test data available.")

    fig = hypothesis_percentage_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No test data available.")

    fig = timing_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No execution time data available.")

    fig = execution_frequency_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No line execution frequency data available.")


if __name__ == "__main__":
    main()
