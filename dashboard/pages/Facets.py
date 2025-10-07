import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Add parent directory to path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analyzer.database import Database

# Page configuration
st.set_page_config(
    page_title="Facets Analysis",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_database():
    """Get database connection."""
    return Database("data/analysis.db")


def main():
    """Facets analysis page."""
    # Sidebar
    with st.sidebar:
        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.header("Test Facets")

    db = get_database()

    # Get overall facets statistics
    with db.connection() as conn:
        # Overall stats
        overall_stats = pd.read_sql_query(
            """
            SELECT
                COUNT(DISTINCT s.node_id) as nodes_with_summaries,
                COUNT(DISTINCT r.id) as repos_with_summaries,
                AVG(LENGTH(s.summary)) as avg_summary_length,
                MIN(LENGTH(s.summary)) as min_summary_length,
                MAX(LENGTH(s.summary)) as max_summary_length
            FROM facets s
            JOIN nodes n ON s.node_id = n.id
            JOIN repositories r ON n.repo_id = r.id
            """,
            conn,
        )

        # Summaries by repository
        repo_summaries = pd.read_sql_query(
            """
            SELECT
                r.owner || '/' || r.name as repository,
                COUNT(DISTINCT n.id) as total_nodes,
                COUNT(DISTINCT s.node_id) as nodes_with_summaries,
                AVG(LENGTH(s.summary)) as avg_summary_length
            FROM repositories r
            LEFT JOIN nodes n ON r.id = n.repo_id
            LEFT JOIN facets s ON n.id = s.node_id
            WHERE r.clone_status = 'success'
            GROUP BY r.id
            HAVING nodes_with_summaries > 0
            ORDER BY nodes_with_summaries DESC
            LIMIT 50
            """,
            conn,
        )

        # Summaries over time
        summary_timeline = pd.read_sql_query(
            """
            SELECT
                DATE(s.created_at) as date,
                COUNT(*) as summaries_created,
                AVG(LENGTH(s.summary)) as avg_length
            FROM facets s
            GROUP BY DATE(s.created_at)
            ORDER BY date
            """,
            conn,
        )

    # Display overall metrics
    if not overall_stats.empty and overall_stats["nodes_with_summaries"].iloc[0] > 0:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Nodes Analyzed",
                f"{overall_stats['nodes_with_summaries'].iloc[0]:,}",
            )
        with col2:
            st.metric(
                "Repositories",
                f"{overall_stats['repos_with_summaries'].iloc[0]:,}",
            )
        with col3:
            avg_length = overall_stats["avg_summary_length"].iloc[0]
            st.metric(
                "Avg Summary Length",
                f"{avg_length:.0f} chars" if avg_length else "0",
            )
        with col4:
            max_length = overall_stats["max_summary_length"].iloc[0]
            st.metric(
                "Max Summary Length",
                f"{max_length:.0f} chars" if max_length else "0",
            )
    else:
        st.info(
            "No facets data available yet. Run the facets experiment to generate data."
        )
        return

    if not repo_summaries.empty:
        st.subheader("Summary Lengths")

        fig = px.histogram(
            repo_summaries,
            x="avg_summary_length",
            nbins=30,
            title="Average Summary Lengths by Repository",
            labels={
                "avg_summary_length": "Average Summary Length (chars)",
                "count": "Number of Repositories",
            },
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Timeline
    if not summary_timeline.empty and len(summary_timeline) > 1:
        st.subheader("Summaries Over Time")

        fig = px.line(
            summary_timeline,
            x="date",
            y="summaries_created",
            title="Summaries Generated Over Time",
            labels={
                "summaries_created": "Summaries Created",
                "date": "Date",
            },
            markers=True,
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Repository selector for detailed view
    st.subheader("Browse Summaries")

    selected_repo = st.selectbox(
        "Select repository",
        repo_summaries["repository"].tolist() if not repo_summaries.empty else [],
        label_visibility="collapsed",
    )

    if selected_repo:
        with db.connection() as conn:
            # Get summaries for selected repository
            summaries = pd.read_sql_query(
                """
                SELECT
                    n.node_id as test_name,
                    n.file_path,
                    n.class_name,
                    n.node_name,
                    s.summary,
                    LENGTH(s.summary) as summary_length,
                    s.created_at
                FROM facets s
                JOIN nodes n ON s.node_id = n.id
                JOIN repositories r ON n.repo_id = r.id
                WHERE r.owner || '/' || r.name = ?
                ORDER BY n.node_id
                """,
                conn,
                params=[selected_repo],
            )

            if not summaries.empty:
                # Display summary count for selected repo
                st.write(f"**{len(summaries)}** summaries found")

                # Show each summary in an expandable section
                for idx, row in summaries.iterrows():
                    with st.expander(f"📝 {row['test_name']}", expanded=False):
                        col1, col2 = st.columns([3, 1])

                        with col1:
                            st.markdown("**Summary:**")
                            st.write(row["summary"])

                        with col2:
                            st.markdown("**Details:**")
                            st.write(f"File: `{row['file_path']}`")
                            if row["class_name"]:
                                st.write(f"Class: `{row['class_name']}`")
                            st.write(f"Length: {row['summary_length']} chars")
                            if row["created_at"]:
                                st.write(f"Created: {row['created_at']}")
            else:
                st.info("No summaries available for this repository.")


if __name__ == "__main__":
    main()
