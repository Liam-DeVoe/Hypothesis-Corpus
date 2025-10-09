import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add parent directory to path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.utils import get_database, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Analysis History",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Analysis history page."""
    # Sidebar
    render_sidebar()

    st.header("Analysis History")

    db = get_database()

    with db.connection() as conn:
        runs = pd.read_sql_query(
            """
            SELECT * FROM analysis_runs
            ORDER BY start_time DESC
            LIMIT 10
        """,
            conn,
        )

    if runs.empty:
        st.info("No analysis runs recorded yet.")
        return

    st.dataframe(runs, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
