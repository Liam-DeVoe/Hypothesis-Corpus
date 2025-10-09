import sys
from pathlib import Path

import streamlit as st

# Add parent directory to path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.utils import (
    create_nodes_per_repo_histogram,
    create_timing_histogram,
    execution_frequency_histogram,
    render_sidebar,
)

# Page configuration
st.set_page_config(
    page_title="Summary",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Summary page with key research findings."""
    # Sidebar
    render_sidebar()

    st.header("Summary")

    fig = create_nodes_per_repo_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No repository data available.")

    fig = create_timing_histogram()
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
