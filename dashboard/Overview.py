import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st

# Add parent directory to path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent.parent))

from analyzer.database import Database

# Page configuration
st.set_page_config(
    page_title="PBT Corpus Analysis Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_database():
    """Get database connection."""
    return Database("data/analysis.db")


def load_data():
    """Load data from database."""
    db = get_database()

    with db.connection() as conn:
        # Repository stats
        repo_stats = conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN clone_status = 'success' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN clone_status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN clone_status = 'pending' THEN 1 ELSE 0 END) as pending
            FROM repositories
            """
        ).fetchone()

        # Node stats
        node_stats = conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
            FROM nodes
            """
        ).fetchone()

        return {
            "repositories": dict(repo_stats),
            "nodes": dict(node_stats),
        }


def render_overview_metrics(stats: dict[str, Any]):
    """Render overview metrics."""
    st.header("Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Repositories",
            stats["repositories"]["total"],
            f"{stats['repositories']['successful']} successful",
        )

    with col2:
        st.metric(
            "Nodes Analyzed",
            stats["nodes"]["total"],
            f"{stats['nodes']['successful']} successful",
        )

    with col3:
        success_rate = (
            stats["repositories"]["successful"] / stats["repositories"]["total"] * 100
            if stats["repositories"]["total"] > 0
            else 0
        )
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%",
            f"{stats['repositories']['failed']} failed",
        )

    # Progress bar
    if stats["repositories"]["total"] > 0:
        progress = (
            stats["repositories"]["successful"] + stats["repositories"]["failed"]
        ) / stats["repositories"]["total"]
        st.progress(progress, text=f"Analysis Progress: {progress*100:.1f}%")


def main():
    """Main dashboard application."""
    # Sidebar
    with st.sidebar:
        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    try:
        stats = load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.info("Make sure the analysis has been run at least once.")
        return

    # Render overview page
    render_overview_metrics(stats)


if __name__ == "__main__":
    main()
