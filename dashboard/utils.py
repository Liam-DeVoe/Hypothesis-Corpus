"""
Shared utilities for dashboard visualizations.
"""

import sys
from datetime import datetime

import numpy as np
import streamlit as st

from analysis.database import get_database as _get_database


def get_database():
    db_dir = "data"
    if "--db-dir" in sys.argv:
        idx = sys.argv.index("--db-dir")
        if idx + 1 < len(sys.argv):
            db_dir = sys.argv[idx + 1]
    return _get_database(db_dir)


def render_sidebar():
    with st.sidebar:
        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def colorbar_ticks(counts):
    """Compute colorbar tickvals/ticktext for log1p-scaled heatmap counts."""
    max_count = int(counts.max())
    ticks = [0]
    v = 10
    while v <= max_count:
        ticks.append(v)
        v *= 10
    ticks.append(max_count)

    def _fmt(n):
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}".rstrip("0").rstrip(".") + "k"
        return str(n)

    return {
        "title": "Count",
        "tickvals": np.log1p(ticks).tolist(),
        "ticktext": [_fmt(t) for t in ticks],
    }


def logbins(data_min, data_max, num_bins=50):
    """Log-spaced bins with integer spacing at the low end.

    Pure logspace bins create visible gaps when data is discrete (integer-valued)
    because no data points exist between consecutive small integers (1-2, 2-3, etc).
    This function uses integer-aligned bin edges where logspace bins would be narrower
    than 1, then switches to logspace for the remainder.
    """
    log_min = np.log10(data_min)
    log_max = np.log10(data_max)

    # Multiplicative step between consecutive logspace bins
    ratio = 10 ** ((log_max - log_min) / num_bins)

    # Below this value, logspace bin width < 1
    threshold = 1 / (ratio - 1)

    # Only use integer bins if at least 2 integers fall below threshold
    if threshold < 2 or data_min >= threshold:
        return np.logspace(log_min, log_max, num_bins + 1)

    # Integer-spaced section
    int_start = max(1, int(np.ceil(data_min)))
    int_end = int(np.floor(min(threshold, data_max)))
    int_edges = np.arange(int_start, int_end + 2, dtype=float)

    parts = []

    # Pre-section: logspace from data_min to int_start (if data starts below 1)
    if data_min < int_start:
        pre_frac = (np.log10(int_start) - log_min) / (log_max - log_min)
        pre_n = max(2, round(num_bins * pre_frac))
        parts.append(np.logspace(log_min, np.log10(int_start), pre_n + 1)[:-1])

    parts.append(int_edges)

    # Post-section: logspace from end of integer section to data_max
    junction = int_edges[-1]
    if junction < data_max:
        post_frac = (log_max - np.log10(junction)) / (log_max - log_min)
        post_n = max(2, round(num_bins * post_frac))
        parts.append(np.logspace(np.log10(junction), log_max, post_n + 1)[1:])

    return np.concatenate(parts)


PLOTLY_CONFIG = {"toImageButtonOptions": {"format": "svg"}}


def plotly_chart(fig, **kwargs):
    """Wrapper around st.plotly_chart with default SVG download config."""
    config = {**PLOTLY_CONFIG, **kwargs.pop("config", {})}
    st.plotly_chart(fig, config=config, **kwargs)


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
