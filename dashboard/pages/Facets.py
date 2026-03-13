import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analysis.experiments.utils import filepath_from_node
from dashboard.utils import get_database, plotly_chart, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Facets Analysis",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Facets analysis page."""
    # Sidebar
    render_sidebar()

    st.header("Test Facets")

    db = get_database()

    # Get overall facets statistics
    # Overall stats
    overall_stats = pd.read_sql_query(
        """
        SELECT
            COUNT(DISTINCT s.node_id) as nodes_with_summaries,
            COUNT(DISTINCT r.id) as repos_with_summaries,
            (SELECT COUNT(*) FROM core_node WHERE canonical_parametrization = TRUE) as total_nodes,
            (SELECT COUNT(*) FROM core_repository WHERE status = 'valid') as total_repos,
            AVG(LENGTH(s.facet)) as avg_summary_length,
            MIN(LENGTH(s.facet)) as min_summary_length,
            MAX(LENGTH(s.facet)) as max_summary_length
        FROM facets_nodes s
        JOIN core_node n ON s.node_id = n.id
        JOIN core_repository r ON n.repo_id = r.id
        WHERE s.type = 'summary' AND n.canonical_parametrization = TRUE
        """,
        db._conn,
    )

    # Pattern stats
    pattern_stats = pd.read_sql_query(
        """
        SELECT
            COUNT(DISTINCT node_id) as nodes_with_patterns,
            COUNT(*) as total_patterns
        FROM facets_nodes
        WHERE type = 'pattern'
        """,
        db._conn,
    )

    # Get total pattern count first
    pattern_count_result = db.fetchone(
        """
        SELECT COUNT(DISTINCT facet) as total
        FROM facets_nodes
        WHERE type = 'pattern'
        """
    )
    total_patterns = pattern_count_result[0] if pattern_count_result else 0

    # Domain stats
    domain_stats = pd.read_sql_query(
        """
        SELECT
            COUNT(DISTINCT node_id) as nodes_with_domains,
            COUNT(*) as total_domains
        FROM facets_nodes
        WHERE type = 'domain'
        """,
        db._conn,
    )

    # Get total domain count first
    domain_count_result = db.fetchone(
        """
        SELECT COUNT(DISTINCT facet) as total
        FROM facets_nodes
        WHERE type = 'domain'
        """
    )
    total_domains = domain_count_result[0] if domain_count_result else 0

    # Get total repo summary count first
    repo_summary_count_result = db.fetchone(
        """
        SELECT COUNT(DISTINCT r.id) as total
        FROM core_repository r
        JOIN facets_repository s ON r.id = s.repo_id
        WHERE s.type = 'summary'
        """
    )
    total_repo_summaries = (
        repo_summary_count_result[0] if repo_summary_count_result else 0
    )

    # Also get repo summaries for the dropdown (limited for performance)
    repo_summaries = pd.read_sql_query(
        """
        SELECT
            r.full_name as repository,
            COUNT(DISTINCT n.id) as total_nodes,
            COUNT(DISTINCT s.node_id) as nodes_with_summaries,
            AVG(LENGTH(s.facet)) as avg_summary_length
        FROM core_repository r
        LEFT JOIN core_node n ON r.id = n.repo_id
        LEFT JOIN facets_nodes s ON n.id = s.node_id
        GROUP BY r.id
        HAVING nodes_with_summaries > 0
        ORDER BY nodes_with_summaries DESC
        LIMIT 50
        """,
        db._conn,
    )

    # Display overall metrics
    if not overall_stats.empty and overall_stats["nodes_with_summaries"].iloc[0] > 0:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            nodes_processed = overall_stats["nodes_with_summaries"].iloc[0]
            total_nodes = overall_stats["total_nodes"].iloc[0]
            st.metric(
                "Nodes processed (canonical only)",
                f"{nodes_processed:,} / {total_nodes:,}",
            )
        with col2:
            repos_processed = overall_stats["repos_with_summaries"].iloc[0]
            total_repos = overall_stats["total_repos"].iloc[0]
            st.metric(
                "Repositories processed",
                f"{repos_processed:,} / {total_repos:,}",
            )
        with col3:
            if not pattern_stats.empty:
                st.metric(
                    "Unique patterns",
                    f"{total_patterns:,}",
                )
        with col4:
            if not domain_stats.empty:
                st.metric(
                    "Unique domains",
                    f"{total_domains:,}",
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
        plotly_chart(fig, width="stretch")

    # Pattern and Domain analysis
    st.subheader("Patterns")

    if total_patterns > 0:
        (col1,) = st.columns(1)

        with col1:
            # Show pattern table with pagination
            st.markdown("**All Patterns**")

            # Initialize pagination state for patterns
            if "patterns_page" not in st.session_state:
                st.session_state.patterns_page = 0

            rows_per_page = 50
            total_pages = (total_patterns + rows_per_page - 1) // rows_per_page

            # Fetch only the current page from database
            offset = st.session_state.patterns_page * rows_per_page
            patterns = pd.read_sql_query(
                """
                SELECT
                    facet as pattern,
                    COUNT(*) as count
                FROM facets_nodes
                WHERE type = 'pattern'
                GROUP BY facet
                ORDER BY count DESC
                LIMIT ? OFFSET ?
                """,
                db._conn,
                params=[rows_per_page, offset],
            )

            # Display current page data
            st.dataframe(
                patterns,
                column_config={
                    "pattern": "Pattern",
                    "count": st.column_config.NumberColumn("Tests", format="%d"),
                },
                hide_index=True,
                width="stretch",
                height=568,
            )

            # Pagination controls
            col_prev, col_info, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button(
                    "← Previous",
                    key="patterns_prev",
                    disabled=st.session_state.patterns_page == 0,
                ):
                    st.session_state.patterns_page -= 1
                    st.rerun()
            with col_info:
                st.markdown(
                    f"<div style='text-align: center'>Page {st.session_state.patterns_page + 1} of {total_pages} ({total_patterns:,} total patterns)</div>",
                    unsafe_allow_html=True,
                )
            with col_next:
                if st.button(
                    "Next →",
                    key="patterns_next",
                    disabled=st.session_state.patterns_page >= total_pages - 1,
                ):
                    st.session_state.patterns_page += 1
                    st.rerun()
    else:
        st.info("No pattern data available yet.")

    st.subheader("Domains")

    if total_domains > 0:
        (col1,) = st.columns(1)

        with col1:
            # Show domain table with pagination
            st.markdown("**All Domains**")

            # Initialize pagination state for domains
            if "domains_page" not in st.session_state:
                st.session_state.domains_page = 0

            rows_per_page = 50
            total_pages = (total_domains + rows_per_page - 1) // rows_per_page

            # Fetch only the current page from database
            offset = st.session_state.domains_page * rows_per_page
            domains = pd.read_sql_query(
                """
                SELECT
                    facet as domain,
                    COUNT(*) as count
                FROM facets_nodes
                WHERE type = 'domain'
                GROUP BY facet
                ORDER BY count DESC
                LIMIT ? OFFSET ?
                """,
                db._conn,
                params=[rows_per_page, offset],
            )

            # Display current page data
            st.dataframe(
                domains,
                column_config={
                    "domain": "Domain",
                    "count": st.column_config.NumberColumn("Tests", format="%d"),
                },
                hide_index=True,
                width="stretch",
                height=568,
            )

            # Pagination controls
            col_prev, col_info, col_next = st.columns([1, 2, 1])
            with col_prev:
                if st.button(
                    "← Previous",
                    key="domains_prev",
                    disabled=st.session_state.domains_page == 0,
                ):
                    st.session_state.domains_page -= 1
                    st.rerun()
            with col_info:
                st.markdown(
                    f"<div style='text-align: center'>Page {st.session_state.domains_page + 1} of {total_pages} ({total_domains:,} total domains)</div>",
                    unsafe_allow_html=True,
                )
            with col_next:
                if st.button(
                    "Next →",
                    key="domains_next",
                    disabled=st.session_state.domains_page >= total_pages - 1,
                ):
                    st.session_state.domains_page += 1
                    st.rerun()
    else:
        st.info("No domain data available yet.")

    # Repository Summaries Section
    st.subheader("Repository Summaries")

    if total_repo_summaries > 0:
        # Initialize pagination state for repository summaries
        if "repo_summaries_page" not in st.session_state:
            st.session_state.repo_summaries_page = 0

        rows_per_page = 50
        total_pages = (total_repo_summaries + rows_per_page - 1) // rows_per_page

        # Calculate start and end indices
        start_idx = st.session_state.repo_summaries_page * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_repo_summaries)

        # Display pagination info
        st.write(
            f"**{total_repo_summaries}** repositories with summaries (showing {start_idx + 1}-{end_idx})"
        )

        # Fetch only the current page from database
        offset = st.session_state.repo_summaries_page * rows_per_page
        repo_level_summaries = pd.read_sql_query(
            """
            SELECT
                r.full_name as repository,
                s.facet as summary,
                LENGTH(s.facet) as summary_length,
                s.created_at
            FROM facets_repository s
            JOIN core_repository r ON s.repo_id = r.id
            WHERE s.type = 'summary'
            ORDER BY r.full_name
            LIMIT ? OFFSET ?
            """,
            db._conn,
            params=[rows_per_page, offset],
        )

        # Display current page of summaries
        for _, row in repo_level_summaries.iterrows():
            with st.expander(f"{row['repository']}", expanded=False):
                st.markdown("**Repository Summary:**")
                st.write(row["summary"])
                st.markdown(f"*Length: {row['summary_length']} chars*")
                if row["created_at"]:
                    st.markdown(f"*Created: {row['created_at']}*")

        # Pagination controls
        col_prev, col_page_info, col_next = st.columns([1, 2, 1])
        with col_prev:
            if st.button(
                "← Previous",
                key="repo_summaries_prev",
                disabled=st.session_state.repo_summaries_page == 0,
            ):
                st.session_state.repo_summaries_page -= 1
                st.rerun()
        with col_page_info:
            st.markdown(
                f"<div style='text-align: center'>Page {st.session_state.repo_summaries_page + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )
        with col_next:
            if st.button(
                "Next →",
                key="repo_summaries_next",
                disabled=st.session_state.repo_summaries_page >= total_pages - 1,
            ):
                st.session_state.repo_summaries_page += 1
                st.rerun()
    else:
        st.info("No repository-level summaries available yet.")

    # Test-level Summaries Section
    st.subheader("Test Summaries")

    selected_repo = st.selectbox(
        "Select repository",
        repo_summaries["repository"].tolist() if not repo_summaries.empty else [],
        label_visibility="collapsed",
    )

    if selected_repo:
        # Get summaries for selected repository
        summaries = pd.read_sql_query(
            """
            SELECT
                n.id as node_db_id,
                n.node_id as test_name,
                s.facet as summary,
                LENGTH(s.facet) as summary_length,
                s.created_at
            FROM facets_nodes s
            JOIN core_node n ON s.node_id = n.id
            JOIN core_repository r ON n.repo_id = r.id
            WHERE r.full_name = ?
                AND s.type = 'summary'
            ORDER BY n.node_id
            """,
            db._conn,
            params=[selected_repo],
        )

        if not summaries.empty:
            # Display summary count for selected repo
            st.write(f"**{len(summaries)}** tests found")

            # Show each summary in an expandable section
            for _idx, row in summaries.iterrows():
                # Get patterns for this test
                patterns_for_test = pd.read_sql_query(
                    """
                    SELECT facet as pattern
                    FROM facets_nodes
                    WHERE node_id = ? AND type = 'pattern'
                    ORDER BY id
                    """,
                    db._conn,
                    params=[row["node_db_id"]],
                )

                # Get domains for this test
                domains_for_test = pd.read_sql_query(
                    """
                    SELECT facet as domain
                    FROM facets_nodes
                    WHERE node_id = ? AND type = 'domain'
                    ORDER BY id
                    """,
                    db._conn,
                    params=[row["node_db_id"]],
                )

                with st.expander(f"📝 {row['test_name']}", expanded=False):
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        st.markdown("**Summary:**")
                        st.write(row["summary"])

                        if not patterns_for_test.empty:
                            st.markdown("**Property Patterns:**")
                            for pat_row in patterns_for_test.iterrows():
                                st.markdown(f"- {pat_row[1]['pattern']}")

                        if not domains_for_test.empty:
                            st.markdown("**Domains:**")
                            for dom_row in domains_for_test.iterrows():
                                st.markdown(f"- {dom_row[1]['domain']}")

                    with col2:
                        st.markdown("**Details:**")
                        file_path = filepath_from_node(row["test_name"])
                        st.write(f"File: `{file_path}`")
                        st.write(f"Length: {row['summary_length']} chars")
                        if not patterns_for_test.empty:
                            st.write(f"Patterns: {len(patterns_for_test)}")
                        if not domains_for_test.empty:
                            st.write(f"Domains: {len(domains_for_test)}")
                        if row["created_at"]:
                            st.write(f"Created: {row['created_at']}")
        else:
            st.info("No summaries available for this repository.")


if __name__ == "__main__":
    main()
