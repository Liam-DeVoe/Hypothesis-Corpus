import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analysis.database import Database
from dashboard.utils import get_database, plotly_chart, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Facet Clusters",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Facet clusters visualization page."""
    # Sidebar
    render_sidebar()

    st.header("Facet Clusters")

    db = get_database()

    # Check if clustering has been run
    result = db.fetchone("SELECT COUNT(*) as count FROM facets_cluster")
    cluster_count = result["count"] if result else 0

    if cluster_count == 0:
        st.info(
            "No clusters found. Run the clustering post-processor to generate clusters:\n\n"
            "```bash\n"
            "python run.py task run cluster\n"
            "```"
        )
        return

    # Tabs for patterns and domains
    pattern_tab, domain_tab = st.tabs(["Pattern", "Domain"])

    with pattern_tab:
        display_clusters(db, "pattern")

    with domain_tab:
        display_clusters(db, "domain")


def display_clusters(db: Database, facet_type: str):
    """Display clusters for a given facet type."""
    # Get cluster summaries
    clusters = pd.read_sql_query(
        """
        SELECT
            cluster_id,
            cluster_name,
            cluster_description,
            num_items,
            created_at
        FROM facets_cluster
        WHERE facet_type = ?
        ORDER BY num_items DESC
        """,
        db._conn,
        params=[facet_type],
    )

    if clusters.empty:
        st.info(f"No {facet_type} clusters found.")
        return

    # Display metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Clusters", len(clusters))
    with col2:
        st.metric("Total Items", clusters["num_items"].sum())
    with col3:
        st.metric("Avg Items per Cluster", f"{clusters['num_items'].mean():.1f}")

    # Cluster size distribution
    st.subheader("Cluster Size Distribution")
    fig = px.histogram(
        clusters,
        x="num_items",
        nbins=20,
        title=f"{facet_type.title()} Cluster Sizes",
        labels={
            "num_items": "Number of Items in Cluster",
            "count": "Number of Clusters",
        },
    )
    fig.update_layout(height=400)
    plotly_chart(fig, width="stretch")

    # Top clusters
    st.subheader("Top Clusters by Size")
    fig = px.bar(
        clusters.head(15),
        x="num_items",
        y="cluster_name",
        orientation="h",
        title=f"Top 15 {facet_type.title()} Clusters",
        labels={"num_items": "Number of Items", "cluster_name": "Cluster"},
        hover_data=["cluster_description"],
    )
    fig.update_layout(height=600, yaxis={"categoryorder": "total ascending"})
    plotly_chart(fig, width="stretch")

    # Detailed cluster view
    st.subheader("Cluster Details")

    # Cluster selector
    cluster_names = [
        f"{row['cluster_name']} ({row['num_items']} items)"
        for _, row in clusters.iterrows()
    ]
    selected_cluster_display = st.selectbox(
        "Select a cluster to view details",
        cluster_names,
        label_visibility="collapsed",
    )

    if selected_cluster_display:
        # Extract cluster name
        selected_cluster_name = selected_cluster_display.rsplit(" (", 1)[0]
        cluster_info = clusters[clusters["cluster_name"] == selected_cluster_name].iloc[
            0
        ]

        # Display cluster info
        st.markdown(f"**{cluster_info['cluster_name']}**")
        st.markdown(f"*{cluster_info['cluster_description']}*")
        st.markdown(f"**Size:** {cluster_info['num_items']} items")

        # Get items in this cluster
        cluster_items = pd.read_sql_query(
            """
            SELECT
                fca.facet_text,
                COUNT(f.node_id) as usage_count
            FROM facets_cluster_assignment fca
            JOIN facets_nodes f ON fca.facet_text = f.facet AND fca.facet_type = f.type
            WHERE fca.cluster_id = ? AND fca.facet_type = ?
            GROUP BY fca.facet_text
            ORDER BY usage_count DESC
            """,
            db._conn,
            params=[int(cluster_info["cluster_id"]), facet_type],
        )

        if not cluster_items.empty:
            st.markdown("**Items in this cluster:**")

            # Show as expandable sections
            for idx, item in cluster_items.iterrows():
                with st.expander(
                    f"{item['facet_text']} (used {item['usage_count']}x)",
                    expanded=False,
                ):
                    # Get tests using this facet
                    tests = pd.read_sql_query(
                        """
                        SELECT DISTINCT
                            n.node_id,
                            r.full_name as repository
                        FROM facets_nodes f
                        JOIN core_node n ON f.node_id = n.id
                        JOIN core_repository r ON n.repo_id = r.id
                        WHERE f.facet = ? AND f.type = ?
                        LIMIT 10
                        """,
                        db._conn,
                        params=[item["facet_text"], facet_type],
                    )

                    if not tests.empty:
                        st.markdown("**Example tests using this facet:**")
                        for _, test in tests.iterrows():
                            st.markdown(f"- `{test['repository']}`: {test['node_id']}")
                        if len(tests) == 10:
                            st.markdown("*(showing first 10 tests)*")


if __name__ == "__main__":
    main()
