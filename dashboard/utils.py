"""
Shared utilities for dashboard visualizations.
"""

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from analysis.database import Database


@st.cache_resource
def get_database():
    return Database("data/data.db")


def render_sidebar():
    with st.sidebar:
        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def common_prefix(strings: list[str]) -> str:
    if not strings:
        return ""
    if len(strings) == 1:
        return ""

    # Find the shortest string length
    min_len = min(len(s) for s in strings)
    if min_len == 0:
        return ""

    # Find common prefix
    prefix = ""
    for i in range(min_len):
        char = strings[0][i]
        if all(s[i] == char for s in strings):
            prefix += char
        else:
            break

    return prefix


def create_timing_histogram():
    """
    Create a histogram showing distribution of test execution times.
    Fetches data from database and returns the figure.

    Returns:
        Plotly figure object or None if no data available
    """
    db = get_database()

    with db.connection() as conn:
        execution_times = pd.read_sql_query(
            """
            SELECT execution_time
            FROM runtime_summary
            WHERE execution_time IS NOT NULL
            """,
            conn,
        )

    if execution_times.empty or len(execution_times) == 0:
        return None

    fig = px.histogram(
        execution_times,
        x="execution_time",
        nbins=50,
        title="Nodes by execution time",
        labels={
            "execution_time": "Execution Time (seconds)",
            "count": "Frequency",
        },
    )
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Execution time (seconds)",
        yaxis_title="Node count",
    )
    return fig


def execution_frequency_histogram():
    """
    Create a histogram showing aggregated line execution frequency distribution
    across all nodes. Fetches data from database and returns the figure.

    Returns:
        Plotly figure object or None if no data available
    """
    import json

    db = get_database()

    with db.connection() as conn:
        all_line_execution_data = pd.read_sql_query(
            """
            SELECT
                rs.line_execution_counts,
                rs.examples_count
            FROM runtime_summary rs
            WHERE rs.line_execution_counts IS NOT NULL
            """,
            conn,
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

        # Get total examples for this node
        total_examples = (
            row["examples_count"]
            if pd.notna(row["examples_count"])
            else max(all_counts)
        )

        # Calculate execution frequencies as percentages
        frequencies = [count / total_examples * 100 for count in all_counts]
        all_frequencies.extend(frequencies)

    if not all_frequencies:
        return None

    # Create a single aggregated histogram
    fig = px.histogram(
        x=all_frequencies,
        nbins=50,
        title="Line Execution Frequency Distribution",
        labels={
            "x": "Line execution frequency (% of total test cases)",
            "count": "Line count",
        },
    )
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Line execution frequency (% of total test cases)",
        yaxis_title="Line count",
    )
    return fig


def create_nodes_per_repo_histogram():
    """
    Create a histogram showing the distribution of node counts per repository.
    Fetches data from database and returns the figure.

    Returns:
        Plotly figure object or None if no data available
    """
    db = get_database()

    with db.connection() as conn:
        repo_node_counts = pd.read_sql_query(
            """
            SELECT
                r.full_name as repo_name,
                COUNT(DISTINCT t.id) as node_count
            FROM core_repositories r
            LEFT JOIN core_nodes t ON r.id = t.repo_id
            GROUP BY r.id
            HAVING node_count > 0
            """,
            conn,
        )

    if repo_node_counts.empty:
        return None

    # Create histogram of node counts
    fig = px.histogram(
        repo_node_counts,
        x="node_count",
        nbins=30,
        title="Node count by repository",
        labels={
            "node_count": "Node count",
            "count": "Repository count",
        },
    )
    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Number of nodes",
        yaxis_title="Repository count",
    )
    return fig
