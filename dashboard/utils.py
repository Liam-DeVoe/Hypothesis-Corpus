"""
Shared utilities for dashboard visualizations.
"""

import sys
from datetime import datetime

import numpy as np
import streamlit as st

from analysis.database import get_database as _get_database


def get_database():
    db_path = "analysis/data.db"
    if "--db-path" in sys.argv:
        idx = sys.argv.index("--db-path")
        if idx + 1 < len(sys.argv):
            db_path = sys.argv[idx + 1]
    return _get_database(db_path)


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
