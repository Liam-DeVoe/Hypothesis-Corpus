import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.utils import (
    common_prefix,
    execution_frequency_histogram,
    get_database,
    render_sidebar,
)

# Page configuration
st.set_page_config(
    page_title="Coverage Analysis",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


def main():
    """Coverage analysis page."""
    # Sidebar
    render_sidebar()

    st.header("Coverage")

    db = get_database()

    # Get overall coverage statistics
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

    # Coverage by repository
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
        HAVING nodes_with_coverage > 0
        ORDER BY total_nodes DESC
        LIMIT 50
        """,
        db._conn,
    )

    # Coverage over time
    coverage_timeline = pd.read_sql_query(
        """
        SELECT
            DATE(rs.executed_at) as date,
            COUNT(DISTINCT rs.node_id) as nodes_count,
            SUM(rs.total_lines_covered) as lines_covered
        FROM runtime_summary rs
        WHERE rs.coverage IS NOT NULL
        GROUP BY DATE(rs.executed_at)
        ORDER BY date
        """,
        db._conn,
    )

    # Test execution results
    test_results = pd.read_sql_query(
        """
        SELECT
            COUNT(*) as total_executions,
            SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN passed = 0 THEN 1 ELSE 0 END) as failed
        FROM runtime_summary
        """,
        db._conn,
    )

    # Execution time distribution
    execution_times = pd.read_sql_query(
        """
        SELECT execution_time
        FROM runtime_summary
        WHERE execution_time IS NOT NULL
        """,
        db._conn,
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
            avg_lines = overall_stats["avg_lines_per_node"].iloc[0]
            st.metric(
                "Avg Lines/Node",
                f"{avg_lines:.0f}" if avg_lines else "0",
            )

    # Coverage by repository chart
    if not repo_coverage.empty:
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

        fig.update_layout(
            title="Coverage Metrics Over Time",
            xaxis_title="Date",
            yaxis={"title": "Lines Covered", "side": "left"},
            hovermode="x unified",
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    # Test execution results
    if not test_results.empty and test_results["total_executions"].iloc[0] > 0:
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

        # Execution time histogram
        if not execution_times.empty and len(execution_times) > 0:
            # Create histogram directly since we already have the data
            fig = px.histogram(
                execution_times,
                x="execution_time",
                nbins=50,
                title="Nodes by execution time",
                labels={
                    "execution_time": "Execution Time (seconds)",
                    "count": "Frequency",
                },
            )
            fig.update_layout(
                showlegend=False,
                height=400,
                xaxis_title="Execution time (seconds)",
                yaxis_title="Node count",
            )
            st.plotly_chart(fig, use_container_width=True)

    fig = execution_frequency_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No line execution frequency data available.")

    # Repository selector for both cumulative coverage and details
    selected_repo = st.selectbox(
        "Select repository",
        repo_coverage["repository"].tolist() if not repo_coverage.empty else [],
        label_visibility="collapsed",
    )

    if selected_repo:
        # Cumulative coverage over test cases
        st.subheader("Cumulative coverage")

        # Get test case coverage data
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
            testcase_data["coverage_parsed"] = testcase_data["coverage"].apply(
                json.loads
            )

            def calc_cumulative(group):
                cumulative_coverage = set()
                cumulative_lines = []
                for coverage in group["coverage_parsed"]:
                    # Add all lines from this testcase to cumulative set
                    for lines in coverage.values():
                        cumulative_coverage.update(lines)
                    cumulative_lines.append(len(cumulative_coverage))
                group["cumulative_lines"] = cumulative_lines
                return group

            cumulative_df = testcase_data.groupby("node_id", group_keys=False).apply(
                calc_cumulative
            )

            # Create cumulative coverage chart
            fig = go.Figure()
            # Get all unique tests for this repository
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

            st.plotly_chart(fig, use_container_width=True)

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

                # Flatten nested dict structure: {"file": {"line": count}} -> [counts]
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

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No line execution frequency data available for this repository.")

        # Detailed test coverage view
        st.subheader("Details")
        # Get test coverage details for selected repository
        runtime_summary_details = pd.read_sql_query(
            """
            SELECT
                t.node_id,
                rs.total_lines_covered,
                rs.passed,
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
                    "passed": st.column_config.CheckboxColumn("Passed", width="small"),
                    "execution_time": st.column_config.NumberColumn(
                        "Time (s)", format="%.2f", width="small"
                    ),
                },
            )
        else:
            st.info("No coverage data available for this repository.")


if __name__ == "__main__":
    main()
