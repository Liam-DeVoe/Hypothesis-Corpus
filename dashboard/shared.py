"""Shared utility functions for dashboard pages."""

import json

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from scipy import stats

from dashboard.utils import get_database


def histogram_with_kde(
    data: list,
    title: str,
    xaxis_title: str,
    yaxis_title: str,
    bin_size: float = 1,
    height: int = 400,
    x_type: str = "linear",
) -> go.Figure:
    """Create a histogram with KDE overlay.

    Args:
        data: List of numerical values to plot
        title: Chart title
        xaxis_title: X-axis label
        yaxis_title: Y-axis label
        bin_size: Size of histogram bins (default: 1)
        height: Chart height in pixels (default: 400)
        x_type: X-axis scale type: "linear" or "log" (default: "linear")

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

    # Add KDE overlay (only if we have enough unique values)
    kde_data = [x for x in data if np.isfinite(x)]
    unique_values = len(set(kde_data))
    if unique_values > 1:
        kde = stats.gaussian_kde(kde_data)
        x_range = np.linspace(min(kde_data), max(kde_data), 200)
        kde_values = kde(x_range)
        # Scale KDE to match histogram counts
        kde_scaled = kde_values * len(kde_data) * bin_size

        fig.add_trace(
            go.Scatter(
                x=x_range,
                y=kde_scaled,
                mode="lines",
                name="KDE",
                line={"color": "rgba(255, 127, 14, 0.8)", "width": 1.5},
                visible="legendonly",
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        height=height,
        showlegend=True,
    )

    if x_type != "linear":
        fig.update_xaxes(type=x_type)

    return fig


def execution_frequency_histogram():
    """Generate line execution frequency distribution histogram."""
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
