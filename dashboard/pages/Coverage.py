import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from analyzer.database import Database

# Page configuration
st.set_page_config(
    page_title="Coverage Analysis",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_database():
    """Get database connection."""
    return Database("data/analysis.db")


def shared_prefix(strings: list[str]) -> str:
    if not strings:
        return ""
    if len(strings) == 1:
        return ""

    # Find the shortest string length
    min_len = min(len(s) for s in strings)
    if min_len == 0:
        return ""

    # Find common prefix
    prefix = ""
    for i in range(min_len):
        char = strings[0][i]
        if all(s[i] == char for s in strings):
            prefix += char
        else:
            break

    return prefix


def main():
    """Coverage analysis page."""
    # Sidebar
    with st.sidebar:
        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    st.header("Coverage")

    db = get_database()

    # Get overall coverage statistics
    with db.connection() as conn:
        # Overall coverage stats
        overall_stats = pd.read_sql_query(
            """
            SELECT
                COUNT(DISTINCT tc.node_id) as nodes_with_coverage,
                COUNT(DISTINCT tc.file_path) as files_covered,
                SUM(tc.covered_lines) as total_lines_covered,
                AVG(tc.covered_lines) as avg_lines_per_file
            FROM node_coverage tc
            """,
            conn,
        )

        # Coverage by repository
        repo_coverage = pd.read_sql_query(
            """
            SELECT
                r.owner || '/' || r.name as repository,
                COUNT(DISTINCT t.id) as total_nodes,
                COUNT(DISTINCT tc.node_id) as nodes_with_coverage,
                SUM(tc.covered_lines) as total_lines_covered,
                COUNT(DISTINCT tc.file_path) as files_covered
            FROM repositories r
            LEFT JOIN nodes t ON r.id = t.repo_id
            LEFT JOIN node_coverage tc ON t.id = tc.node_id
            WHERE r.clone_status = 'success'
            GROUP BY r.id
            HAVING nodes_with_coverage > 0
            ORDER BY total_nodes DESC
            LIMIT 50
            """,
            conn,
        )

        # Coverage over time
        coverage_timeline = pd.read_sql_query(
            """
            SELECT
                DATE(tc.collected_at) as date,
                COUNT(DISTINCT tc.node_id) as nodes_count,
                SUM(tc.covered_lines) as lines_covered,
                COUNT(DISTINCT tc.file_path) as files_covered
            FROM node_coverage tc
            GROUP BY DATE(tc.collected_at)
            ORDER BY date
            """,
            conn,
        )

        # Test execution results
        test_results = pd.read_sql_query(
            """
            SELECT
                COUNT(*) as total_executions,
                SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN passed = 0 THEN 1 ELSE 0 END) as failed,
                AVG(execution_time) as avg_execution_time
            FROM node_executions
            """,
            conn,
        )

    # Display overall metrics
    col1, col2 = st.columns(2)

    if not overall_stats.empty:
        with col1:
            st.metric(
                "Node count",
                f"{overall_stats['nodes_with_coverage'].iloc[0]:,}",
            )
        with col2:
            avg_lines = overall_stats["avg_lines_per_file"].iloc[0]
            st.metric(
                "Avg Lines/File",
                f"{avg_lines:.0f}" if avg_lines else "0",
            )

    # Coverage by repository chart
    if not repo_coverage.empty:
        st.subheader("Coverage by Repository")

        fig = px.bar(
            repo_coverage.head(50),
            x="repository",
            y="total_nodes",
            title="Top 50 Repositories by Node Count",
            labels={
                "total_nodes": "Total Nodes",
                "repository": "Repository",
            },
            hover_data=["nodes_with_coverage", "total_lines_covered", "files_covered"],
        )
        fig.update_layout(height=600, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # Coverage timeline
    if not coverage_timeline.empty and len(coverage_timeline) > 1:
        st.subheader("Coverage Over Time")

        fig = go.Figure()

        # Add lines covered as primary metric
        fig.add_trace(
            go.Bar(
                x=coverage_timeline["date"],
                y=coverage_timeline["lines_covered"],
                name="Lines Covered",
                marker_color="lightgreen",
                yaxis="y",
            )
        )

        # Add files covered on secondary axis
        fig.add_trace(
            go.Scatter(
                x=coverage_timeline["date"],
                y=coverage_timeline["files_covered"],
                name="Files Covered",
                mode="lines+markers",
                line={"color": "blue", "width": 2},
                yaxis="y2",
            )
        )

        fig.update_layout(
            title="Coverage Metrics Over Time",
            xaxis_title="Date",
            yaxis={"title": "Lines Covered", "side": "left"},
            yaxis2={"title": "Files Covered", "overlaying": "y", "side": "right"},
            hovermode="x unified",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    # Test execution results
    if not test_results.empty and test_results["total_executions"].iloc[0] > 0:
        st.subheader("Test Execution Results")

        col1, col2 = st.columns(2)

        with col1:
            # Pass/Fail pie chart
            passed = test_results["passed"].iloc[0]
            failed = test_results["failed"].iloc[0]

            fig = px.pie(
                values=[passed, failed],
                names=["Passed", "Failed"],
                title="Test Execution Results",
                color_discrete_map={"Passed": "green", "Failed": "red"},
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Execution statistics
            st.metric(
                "Total Node Executions", f"{test_results['total_executions'].iloc[0]:,}"
            )
            st.metric("Pass Rate", f"{(passed / (passed + failed) * 100):.1f}%")
            avg_time = test_results["avg_execution_time"].iloc[0]
            st.metric("Avg Execution Time", f"{avg_time:.2f}s" if avg_time else "N/A")

    # Repository selector for both cumulative coverage and details
    selected_repo = st.selectbox(
        "Select repository",
        repo_coverage["repository"].tolist() if not repo_coverage.empty else [],
        label_visibility="collapsed",
    )

    if selected_repo:
        # Cumulative coverage over test cases
        st.subheader("Cumulative coverage")

        # Get cumulative coverage data
        with db.connection() as conn:
            cumulative_data = pd.read_sql_query(
                """
                SELECT
                    t.node_id,
                    tcc.case_number,
                    tcc.file_path,
                    tcc.cumulative_count,
                    r.owner || '/' || r.name as repository
                FROM case_coverage tcc
                JOIN nodes t ON tcc.node_id = t.id
                JOIN repositories r ON t.repo_id = r.id
                WHERE r.owner || '/' || r.name = ?
                ORDER BY t.id, tcc.file_path, tcc.case_number
                """,
                conn,
                params=[selected_repo],
            )

        if not cumulative_data.empty:
            # Create cumulative coverage chart
            fig = go.Figure()

            # Get all unique tests for this repository
            node_ids = cumulative_data["node_id"].unique()
            common_prefix = shared_prefix(node_ids.tolist())

            for node_id in node_ids:
                test_data = cumulative_data[cumulative_data["node_id"] == node_id]

                # Aggregate coverage across all files for each case
                aggregated_data = (
                    test_data.groupby("case_number")
                    .agg({"cumulative_count": "sum"})
                    .reset_index()
                )

                test_name = node_id.lstrip(common_prefix)

                fig.add_trace(
                    go.Scatter(
                        x=aggregated_data["case_number"],
                        y=aggregated_data["cumulative_count"],
                        mode="lines",
                        name=test_name,
                        line={"width": 2},
                    )
                )

            fig.update_layout(
                title=f"{selected_repo}",
                xaxis_title="Input count",
                yaxis_title="Line coverage",
                hovermode="x unified",
                height=600,
                showlegend=True,
            )

            st.plotly_chart(fig, use_container_width=True)

        # Detailed test coverage view
        st.subheader("Details")
        with db.connection() as conn:
            # Get test coverage details for selected repository
            node_coverage_details = pd.read_sql_query(
                """
                SELECT
                    t.node_id,
                    tc.file_path,
                    tc.covered_lines,
                    te.passed,
                    te.execution_time
                FROM nodes t
                JOIN repositories r ON t.repo_id = r.id
                LEFT JOIN node_coverage tc ON t.id = tc.node_id
                LEFT JOIN node_executions te ON t.id = te.node_id
                WHERE r.owner || '/' || r.name = ?
                AND tc.covered_lines IS NOT NULL
                ORDER BY tc.covered_lines DESC
                """,
                conn,
                params=[selected_repo],
            )

            if not node_coverage_details.empty:
                st.dataframe(
                    node_coverage_details,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "node_id": st.column_config.TextColumn("Test", width="large"),
                        "file_path": st.column_config.TextColumn(
                            "File", width="medium"
                        ),
                        "covered_lines": st.column_config.NumberColumn(
                            "Lines Covered", width="small"
                        ),
                        "passed": st.column_config.CheckboxColumn(
                            "Passed", width="small"
                        ),
                        "execution_time": st.column_config.NumberColumn(
                            "Time (s)", format="%.2f", width="small"
                        ),
                    },
                )
            else:
                st.info("No coverage data available for this repository.")


if __name__ == "__main__":
    main()
