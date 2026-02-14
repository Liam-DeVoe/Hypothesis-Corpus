import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import execution_frequency_histogram, histogram_with_kde
from dashboard.utils import common_prefix, get_database, render_sidebar

st.set_page_config(
    page_title="Coverage",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    render_sidebar()

    st.header("Coverage")

    db = get_database()

    # Overall coverage stats
    overall_stats = pd.read_sql_query(
        """
        SELECT
            COUNT(DISTINCT rs.node_id) as nodes_with_coverage,
            SUM(rs.total_lines_covered) as total_lines_covered,
            AVG(rs.total_lines_covered) as avg_lines_per_node
        FROM runtime_summary rs
        WHERE rs.coverage IS NOT NULL
        """,
        db._conn,
    )

    col1, col2 = st.columns(2)
    if not overall_stats.empty:
        with col1:
            st.metric(
                "Nodes with coverage",
                f"{overall_stats['nodes_with_coverage'].iloc[0]:,}",
            )
        with col2:
            avg_lines = overall_stats["avg_lines_per_node"].iloc[0]
            st.metric(
                "Avg Lines/Node",
                f"{avg_lines:.0f}" if avg_lines else "0",
            )

    # Max lines covered histogram
    lines_covered = pd.read_sql_query(
        """
        SELECT total_lines_covered
        FROM runtime_summary
        WHERE total_lines_covered IS NOT NULL AND status IN ('passed', 'failed')
        """,
        db._conn,
    )
    if not lines_covered.empty:
        fig = histogram_with_kde(
            data=lines_covered["total_lines_covered"].tolist(),
            title="Max lines covered per test",
            xaxis_title="Total lines covered",
            yaxis_title="Test count",
            bin_size=5,
        )
        st.plotly_chart(fig, width="stretch")

    # Global line execution frequency
    fig = execution_frequency_histogram()
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No line execution frequency data available.")

    # Repository selector
    repo_list = pd.read_sql_query(
        """
        SELECT
            r.full_name as repository,
            COUNT(DISTINCT t.id) as total_nodes,
            COUNT(DISTINCT rs.node_id) as nodes_with_coverage
        FROM core_repository r
        LEFT JOIN core_node t ON r.id = t.repo_id
        LEFT JOIN runtime_summary rs ON t.id = rs.node_id
        GROUP BY r.id
        HAVING nodes_with_coverage > 0
        ORDER BY total_nodes DESC
        LIMIT 50
        """,
        db._conn,
    )

    if repo_list.empty:
        st.info("No coverage data available.")
        return

    selected_repo = st.selectbox(
        "Select repository",
        repo_list["repository"].tolist(),
        label_visibility="collapsed",
    )

    if not selected_repo:
        return

    # Cumulative coverage over test cases
    st.subheader("Cumulative coverage")

    testcase_data = pd.read_sql_query(
        """
        SELECT
            t.node_id,
            tc.testcase_number,
            tc.coverage,
            r.full_name as repository
        FROM runtime_testcase tc
        JOIN core_node t ON tc.node_id = t.id
        JOIN core_repository r ON t.repo_id = r.id
        WHERE r.full_name = ?
        ORDER BY t.id, tc.testcase_number
        """,
        db._conn,
        params=[selected_repo],
    )

    if not testcase_data.empty:
        testcase_data["coverage_parsed"] = testcase_data["coverage"].apply(json.loads)

        def calc_cumulative(group):
            cumulative_coverage = set()
            cumulative_lines = []
            for coverage in group["coverage_parsed"]:
                for lines in coverage.values():
                    cumulative_coverage.update(lines)
                cumulative_lines.append(len(cumulative_coverage))
            group["cumulative_lines"] = cumulative_lines
            return group

        cumulative_df = testcase_data.groupby("node_id", group_keys=False)[
            ["node_id", "testcase_number", "coverage_parsed"]
        ].apply(calc_cumulative, include_groups=False)

        fig = go.Figure()
        node_ids = cumulative_df["node_id"].unique()
        prefix = common_prefix(node_ids.tolist())

        for node_id in node_ids:
            test_data = cumulative_df[cumulative_df["node_id"] == node_id]
            test_name = node_id.lstrip(prefix)

            fig.add_trace(
                go.Scatter(
                    x=test_data["testcase_number"],
                    y=test_data["cumulative_lines"],
                    mode="lines",
                    name=test_name,
                    line={"width": 2},
                )
            )

        fig.update_layout(
            title=f"{selected_repo}",
            xaxis_title="Test case count",
            yaxis_title="Line coverage",
            hovermode="x unified",
            height=600,
            showlegend=True,
        )

        st.plotly_chart(fig, width="stretch")

    # Line execution frequency histograms
    st.subheader("Line execution frequency distribution")

    line_execution_data = pd.read_sql_query(
        """
        SELECT
            t.node_id,
            rs.line_execution_counts,
            rs.count_test_cases
        FROM runtime_summary rs
        JOIN core_node t ON rs.node_id = t.id
        JOIN core_repository r ON t.repo_id = r.id
        WHERE r.full_name = ?
        AND rs.line_execution_counts IS NOT NULL
        """,
        db._conn,
        params=[selected_repo],
    )

    if not line_execution_data.empty:
        fig = go.Figure()

        node_ids = line_execution_data["node_id"].unique()
        prefix = common_prefix(node_ids.tolist())

        for _, row in line_execution_data.iterrows():
            node_id = row["node_id"]
            line_counts_dict = json.loads(row["line_execution_counts"])

            if not line_counts_dict:
                continue

            all_counts = []
            for file_counts in line_counts_dict.values():
                all_counts.extend(file_counts.values())

            if not all_counts:
                continue

            total_test_cases = row["count_test_cases"]
            frequencies = [count / total_test_cases * 100 for count in all_counts]
            test_name = node_id.lstrip(prefix)

            fig.add_trace(
                go.Histogram(
                    x=frequencies,
                    name=test_name,
                    opacity=0.6,
                    nbinsx=50,
                )
            )

        fig.update_layout(
            title=f"{selected_repo}",
            xaxis_title="Line execution frequency (% of total test cases)",
            yaxis_title="Line count",
            barmode="overlay",
            hovermode="x unified",
            height=600,
            showlegend=True,
        )

        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No line execution frequency data available for this repository.")

    # Detailed test coverage view
    st.subheader("Details")
    runtime_summary_details = pd.read_sql_query(
        """
        SELECT
            t.node_id,
            rs.total_lines_covered,
            rs.status,
            rs.execution_time
        FROM core_node t
        JOIN core_repository r ON t.repo_id = r.id
        LEFT JOIN runtime_summary rs ON t.id = rs.node_id
        WHERE r.full_name = ?
        AND rs.coverage IS NOT NULL
        ORDER BY rs.total_lines_covered DESC
        """,
        db._conn,
        params=[selected_repo],
    )

    if not runtime_summary_details.empty:
        st.dataframe(
            runtime_summary_details,
            width="stretch",
            hide_index=True,
            column_config={
                "node_id": st.column_config.TextColumn("Test", width="large"),
                "total_lines_covered": st.column_config.NumberColumn(
                    "Lines Covered", width="small"
                ),
                "status": st.column_config.TextColumn("Status", width="small"),
                "execution_time": st.column_config.NumberColumn(
                    "Time (s)", format="%.2f", width="small"
                ),
            },
        )
    else:
        st.info("No coverage data available for this repository.")


if __name__ == "__main__":
    main()
