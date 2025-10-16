"""
Shared utilities for dashboard visualizations.
"""

from datetime import datetime

import streamlit as st

from analysis.database import get_database as _get_database


def get_database():
    return _get_database(st.session_state["db_path"])


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
