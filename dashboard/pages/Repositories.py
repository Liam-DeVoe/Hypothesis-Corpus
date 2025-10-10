import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.utils import get_database, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Repositories",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Repositories page."""
    # Sidebar
    render_sidebar()

    st.header("Repositories")

    db = get_database()

    # Get repository list
    repos = pd.read_sql_query(
        """
        SELECT
            r.full_name as repository,
            COUNT(DISTINCT t.id) as node_count,
            r.created_at
        FROM core_repository r
        LEFT JOIN core_node t ON r.id = t.repo_id
        GROUP BY r.id
        ORDER BY r.created_at DESC
        LIMIT 100
    """,
        db._conn,
    )

    if repos.empty:
        st.info("No repositories processed yet.")
        return

    # Display repository table
    st.dataframe(
        repos,
        width="stretch",
        hide_index=True,
        column_config={
            "repository": st.column_config.TextColumn("Repository", width="medium"),
            "node_count": st.column_config.NumberColumn("Tests", width="small"),
            "created_at": st.column_config.DatetimeColumn(
                "Analyzed At", width="medium"
            ),
        },
    )

    # Repository selection for detailed view
    if not repos.empty:
        selected_repo = st.selectbox("Repository", repos["repository"].tolist())

        if selected_repo:
            # Get test details for selected repository
            node_details = pd.read_sql_query(
                """
                SELECT
                    t.node_id,
                    t.status
                FROM core_node t
                JOIN core_repository r ON t.repo_id = r.id
                WHERE r.full_name = ?
                GROUP BY t.id
            """,
                db._conn,
                params=[selected_repo],
            )

            if not node_details.empty:
                st.subheader(f"{selected_repo}")

                st.dataframe(
                    node_details,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "node_id": st.column_config.TextColumn("Test", width="large"),
                        "status": st.column_config.TextColumn("Status", width="small"),
                    },
                )


if __name__ == "__main__":
    main()
