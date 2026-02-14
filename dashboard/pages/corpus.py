import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import get_database, render_sidebar

st.set_page_config(
    page_title="Corpus",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def hypothesis_test_count_histogram(db):
    repo_counts = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            json_array_length(node_ids) as node_count
        FROM core_repository
        WHERE json_array_length(node_ids) > 0
        """,
        db._conn,
    )
    if repo_counts.empty:
        return None

    return histogram_with_kde(
        data=repo_counts["node_count"].tolist(),
        title="Hypothesis test count by repository",
        xaxis_title="# of Hypothesis tests",
        yaxis_title="Repository count",
        bin_size=1,
        x_type="log",
    )


def unique_test_count_histogram(db):
    repo_counts = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            node_ids
        FROM core_repository
        WHERE json_array_length(node_ids) > 0
        """,
        db._conn,
    )
    if repo_counts.empty:
        return None

    unique_counts = []
    for _, row in repo_counts.iterrows():
        node_ids = json.loads(row["node_ids"])
        unique_tests = set()
        for node_id in node_ids:
            base_test = node_id.split("[")[0]
            unique_tests.add(base_test)
        unique_counts.append(len(unique_tests))

    return histogram_with_kde(
        data=unique_counts,
        title="Hypothesis canonical test count by repository",
        xaxis_title="# of canonical Hypothesis tests",
        yaxis_title="Repository count",
        bin_size=1,
        x_type="log",
    )


def hypothesis_percentage_histogram(db):
    repo_data = pd.read_sql_query(
        """
        SELECT
            full_name as repo_name,
            json_array_length(node_ids) as node_count,
            json_array_length(other_node_ids) as other_node_count,
            CAST(json_array_length(node_ids) AS FLOAT) /
                (json_array_length(node_ids) + json_array_length(other_node_ids)) * 100
                as hypothesis_percentage
        FROM core_repository
        WHERE json_array_length(node_ids) > 0 OR json_array_length(other_node_ids) > 0
        """,
        db._conn,
    )
    if repo_data.empty:
        return None

    return histogram_with_kde(
        data=repo_data["hypothesis_percentage"].tolist(),
        title="% of repository tests that are Hypothesis tests",
        xaxis_title="% of repository tests that are Hypothesis tests",
        yaxis_title="Repository count",
        bin_size=0.5,
    )


def repos_by_node_count(db):
    repo_coverage = pd.read_sql_query(
        """
        SELECT
            r.full_name as repository,
            COUNT(DISTINCT t.id) as total_nodes,
            COUNT(DISTINCT rs.node_id) as nodes_with_coverage,
            SUM(rs.total_lines_covered) as total_lines_covered
        FROM core_repository r
        LEFT JOIN core_node t ON r.id = t.repo_id
        LEFT JOIN runtime_summary rs ON t.id = rs.node_id
        GROUP BY r.id
        HAVING total_nodes > 0
        ORDER BY total_nodes DESC
        LIMIT 50
        """,
        db._conn,
    )
    if repo_coverage.empty:
        return None

    fig = px.bar(
        repo_coverage,
        x="repository",
        y="total_nodes",
        title="Repositories by Node Count",
        labels={
            "total_nodes": "Total Nodes",
            "repository": "Repository",
        },
        hover_data=["nodes_with_coverage", "total_lines_covered"],
    )
    fig.update_layout(height=600, xaxis_tickangle=-45)
    return fig


def main():
    render_sidebar()

    st.header("Corpus")

    db = get_database()

    # Metrics row
    stats = db.fetchone(
        """
        SELECT
            (SELECT COUNT(*) FROM core_repository WHERE status = 'valid') as repos,
            (SELECT COUNT(*) FROM core_node cn
             JOIN core_repository r ON cn.repo_id = r.id
             WHERE r.status = 'valid') as nodes,
            (SELECT COUNT(*) FROM core_node cn
             JOIN core_repository r ON cn.repo_id = r.id
             WHERE r.status = 'valid' AND cn.canonical_parametrization) as canonical
        """
    )
    col1, col2, col3 = st.columns(3)
    col1.metric("Repositories", f"{stats['repos']:,}")
    col2.metric("All Nodes", f"{stats['nodes']:,}")
    col3.metric("Canonical Nodes", f"{stats['canonical']:,}")

    # Histograms
    fig = hypothesis_test_count_histogram(db)
    if fig:
        st.plotly_chart(fig, width="stretch")

    fig = unique_test_count_histogram(db)
    if fig:
        st.plotly_chart(fig, width="stretch")

    fig = hypothesis_percentage_histogram(db)
    if fig:
        st.plotly_chart(fig, width="stretch")

    fig = repos_by_node_count(db)
    if fig:
        st.plotly_chart(fig, width="stretch")

    # Repository browser
    st.divider()
    st.subheader("Browse repositories")

    repos = pd.read_sql_query(
        """
        SELECT
            r.full_name as repository,
            COUNT(DISTINCT t.id) as node_count,
            r.created_at
        FROM core_repository r
        LEFT JOIN core_node t ON r.id = t.repo_id
        WHERE r.status = 'valid'
        GROUP BY r.id
        ORDER BY node_count DESC
        """,
        db._conn,
    )

    if repos.empty:
        st.info("No repositories processed yet.")
        return

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

    selected_repo = st.selectbox("Repository", repos["repository"].tolist())

    if selected_repo:
        node_details = pd.read_sql_query(
            """
            SELECT
                t.node_id,
                COALESCE(rs.status, 'not analyzed') as status
            FROM core_node t
            JOIN core_repository r ON t.repo_id = r.id
            LEFT JOIN runtime_summary rs ON t.id = rs.node_id
            WHERE r.full_name = ?
            GROUP BY t.id
            """,
            db._conn,
            params=[selected_repo],
        )

        if not node_details.empty:
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
