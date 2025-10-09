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
    with db.connection() as conn:
        repos = pd.read_sql_query(
            """
            SELECT
                r.repo_name as repository,
                r.clone_status as status,
                COUNT(DISTINCT t.id) as node_count,
                r.created_at
            FROM repositories r
            LEFT JOIN nodes t ON r.id = t.repo_id
            GROUP BY r.id
            ORDER BY r.created_at DESC
            LIMIT 100
        """,
            conn,
        )

    if repos.empty:
        st.info("No repositories processed yet.")
        return

    # Filter by status
    status_filter = st.selectbox("Status", ["All", "success", "failed"], index=0)

    if status_filter != "All":
        repos = repos[repos["status"] == status_filter]

    # Display repository table
    st.dataframe(
        repos,
        width="stretch",
        hide_index=True,
        column_config={
            "repository": st.column_config.TextColumn("Repository", width="medium"),
            "status": st.column_config.TextColumn("Status", width="small"),
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
            with db.connection() as conn:
                # Get test details for selected repository
                node_details = pd.read_sql_query(
                    """
                    SELECT
                        t.node_id,
                        t.status
                    FROM nodes t
                    JOIN repositories r ON t.repo_id = r.id
                    WHERE r.repo_name = ?
                    GROUP BY t.id
                """,
                    conn,
                    params=[selected_repo],
                )

                if not node_details.empty:
                    st.subheader(f"{selected_repo}")

                    st.dataframe(
                        node_details,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "node_id": st.column_config.TextColumn(
                                "Test", width="large"
                            ),
                            "status": st.column_config.TextColumn(
                                "Status", width="small"
                            ),
                        },
                    )


if __name__ == "__main__":
    main()
