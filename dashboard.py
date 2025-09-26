"""
Streamlit dashboard for visualizing PBT corpus analysis results.
"""

from datetime import datetime
from typing import Any, Dict

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from analyzer.database import Database

# Page configuration
st.set_page_config(
    page_title="PBT Corpus Analysis Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_database():
    """Get database connection."""
    return Database("data/analysis.db")


def load_data():
    """Load data from database."""
    db = get_database()
    stats = db.get_analysis_stats()
    return stats


def render_overview_metrics(stats: Dict[str, Any]):
    """Render overview metrics."""
    st.header("Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Repositories",
            stats["repositories"]["total"],
            f"{stats['repositories']['successful']} successful",
        )

    with col2:
        st.metric(
            "Total Tests Analyzed",
            stats["tests"]["total"],
            f"{stats['tests']['successful']} successful",
        )

    with col3:
        success_rate = (
            stats["repositories"]["successful"] / stats["repositories"]["total"] * 100
            if stats["repositories"]["total"] > 0
            else 0
        )
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%",
            f"{stats['repositories']['failed']} failed",
        )

    with col4:
        pending = stats["repositories"]["pending"]
        st.metric("Pending", pending, "In progress" if pending > 0 else "Complete")

    # Progress bar
    if stats["repositories"]["total"] > 0:
        progress = (
            stats["repositories"]["successful"] + stats["repositories"]["failed"]
        ) / stats["repositories"]["total"]
        st.progress(progress, text=f"Analysis Progress: {progress*100:.1f}%")


def render_generator_analysis(stats: Dict[str, Any]):
    """Render generator usage analysis."""
    st.header("Generator Usage Analysis")

    if not stats.get("top_generators"):
        st.info("No generator data available yet.")
        return

    # Create DataFrame
    df = pd.DataFrame(stats["top_generators"])

    col1, col2 = st.columns([2, 1])

    with col1:
        # Bar chart of top generators
        fig = px.bar(
            df.head(15),
            x="total_uses",
            y="generator_name",
            orientation="h",
            title="Top 15 Most Used Generators",
            labels={"total_uses": "Total Uses", "generator_name": "Generator"},
            color="total_uses",
            color_continuous_scale="Viridis",
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Statistics
        st.subheader("Generator Statistics")

        total_generators = len(df)
        total_uses = df["total_uses"].sum()
        avg_uses = df["total_uses"].mean()

        st.metric("Unique Generators", total_generators)
        st.metric("Total Generator Uses", f"{total_uses:,}")
        st.metric("Average Uses per Generator", f"{avg_uses:.1f}")

        # Top 5 table
        st.subheader("Top 5 Generators")
        top_5 = df.head(5)[["generator_name", "total_uses"]]
        st.dataframe(top_5, hide_index=True)


def render_property_types(stats: Dict[str, Any]):
    """Render property type distribution."""
    st.header("Property Type Distribution")

    if not stats.get("property_types"):
        st.info("No property type data available yet.")
        return

    df = pd.DataFrame(stats["property_types"])

    col1, col2 = st.columns(2)

    with col1:
        # Pie chart
        fig = px.pie(
            df,
            values="count",
            names="property_type",
            title="Property Type Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Bar chart
        fig = px.bar(
            df,
            x="property_type",
            y="count",
            title="Property Types by Count",
            labels={"count": "Number of Tests", "property_type": "Property Type"},
            color="count",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig, use_container_width=True)


def render_feature_usage(stats: Dict[str, Any]):
    """Render feature usage analysis."""
    st.header("Hypothesis Feature Usage")

    if not stats.get("feature_usage"):
        st.info("No feature usage data available yet.")
        return

    df = pd.DataFrame(stats["feature_usage"])

    # Create a more detailed view
    fig = go.Figure()

    # Add bars for test count
    fig.add_trace(
        go.Bar(
            name="Tests Using Feature",
            x=df["feature_name"],
            y=df["test_count"],
            yaxis="y",
            marker_color="lightblue",
        )
    )

    # Add line for total uses
    fig.add_trace(
        go.Scatter(
            name="Total Feature Uses",
            x=df["feature_name"],
            y=df["total_uses"],
            yaxis="y2",
            mode="lines+markers",
            marker_color="red",
            line={"width": 2},
        )
    )

    # Update layout with dual y-axes
    fig.update_layout(
        title="Feature Usage Analysis",
        xaxis_title="Feature",
        yaxis={"title": "Number of Tests", "side": "left"},
        yaxis2={"title": "Total Uses", "overlaying": "y", "side": "right"},
        hovermode="x unified",
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Feature usage table
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Feature Usage Details")
        feature_df = df[["feature_name", "test_count", "total_uses"]]
        feature_df.columns = ["Feature", "Tests", "Total Uses"]
        st.dataframe(feature_df, hide_index=True)

    with col2:
        st.subheader("Feature Insights")

        if len(df) > 0:
            most_used = df.loc[df["total_uses"].idxmax()]
            most_tests = df.loc[df["test_count"].idxmax()]

            st.info(
                f"Most Used Feature: {most_used['feature_name']} ({most_used['total_uses']} uses)"
            )
            st.info(
                f"Most Popular Feature: {most_tests['feature_name']} (in {most_tests['test_count']} tests)"
            )

            # Calculate average uses per test
            df["avg_uses_per_test"] = df["total_uses"] / df["test_count"]
            most_intensive = df.loc[df["avg_uses_per_test"].idxmax()]
            st.info(
                f"Most Intensive Usage: {most_intensive['feature_name']} ({most_intensive['avg_uses_per_test']:.1f} uses/test)"
            )


def render_property_explorer():
    """Render property source code explorer."""
    st.header("Property Source Code Explorer")

    db = get_database()

    # Get tests with property text
    with db.connection() as conn:
        properties = pd.read_sql_query(
            """
            SELECT
                r.owner || '/' || r.name as repository,
                t.node_id,
                t.test_name,
                t.property_text,
                t.github_permalink,
                COUNT(DISTINCT gu.generator_name) as generator_count,
                GROUP_CONCAT(DISTINCT pt.property_type) as property_types
            FROM tests t
            JOIN repositories r ON t.repo_id = r.id
            LEFT JOIN generator_usage gu ON t.id = gu.test_id
            LEFT JOIN property_types pt ON t.id = pt.test_id
            WHERE t.property_text IS NOT NULL
            GROUP BY t.id
            ORDER BY r.name, t.node_id
            """,
            conn,
        )

    if properties.empty:
        st.info("No property source code available yet. Run analysis to collect property implementations.")
        return

    # Repository filter
    repos = properties["repository"].unique().tolist()
    selected_repo = st.selectbox(
        "Select Repository",
        ["All"] + repos,
        index=0
    )

    if selected_repo != "All":
        filtered_props = properties[properties["repository"] == selected_repo]
    else:
        filtered_props = properties

    # Property type filter
    if not filtered_props.empty:
        all_prop_types = set()
        for types_str in filtered_props["property_types"].dropna():
            if types_str:
                all_prop_types.update(types_str.split(","))

        if all_prop_types:
            selected_type = st.selectbox(
                "Filter by Property Type",
                ["All"] + sorted(all_prop_types),
                index=0
            )

            if selected_type != "All":
                filtered_props = filtered_props[
                    filtered_props["property_types"].str.contains(selected_type, na=False)
                ]

    # Display properties
    st.subheader(f"Found {len(filtered_props)} properties")

    for idx, prop in filtered_props.iterrows():
        with st.expander(f"{prop['repository']} - {prop['test_name'] or prop['node_id'].split('::')[-1]}"):
            # Metadata
            col1, col2 = st.columns([3, 1])
            with col1:
                st.caption(f"Node ID: {prop['node_id']}")
                if prop['property_types']:
                    st.caption(f"Types: {prop['property_types']}")
                st.caption(f"Generators: {prop['generator_count']}")
            with col2:
                if prop['github_permalink']:
                    st.link_button(
                        "View on GitHub",
                        prop['github_permalink'],
                        use_container_width=True
                    )

            # Source code
            if prop['property_text']:
                st.code(prop['property_text'], language="python")
            else:
                st.warning("Source code not available")


def render_repository_details():
    """Render detailed repository analysis."""
    st.header("Repository Details")

    db = get_database()

    # Get repository list
    with db.connection() as conn:
        repos = pd.read_sql_query(
            """
            SELECT
                r.owner || '/' || r.name as repository,
                r.clone_status as status,
                COUNT(DISTINCT t.id) as test_count,
                r.created_at
            FROM repositories r
            LEFT JOIN tests t ON r.id = t.repo_id
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
    status_filter = st.selectbox(
        "Filter by Status", ["All", "success", "failed", "pending"], index=0
    )

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
            "test_count": st.column_config.NumberColumn("Tests", width="small"),
            "created_at": st.column_config.DatetimeColumn(
                "Analyzed At", width="medium"
            ),
        },
    )

    # Repository selection for detailed view
    if not repos.empty:
        selected_repo = st.selectbox(
            "Select repository for detailed view", repos["repository"].tolist()
        )

        if selected_repo:
            with db.connection() as conn:
                # Get test details for selected repository
                test_details = pd.read_sql_query(
                    """
                    SELECT
                        t.node_id,
                        t.status,
                        t.github_permalink,
                        CASE WHEN t.property_text IS NOT NULL THEN 'Yes' ELSE 'No' END as has_source,
                        COUNT(DISTINCT gu.generator_name) as generator_count,
                        COUNT(DISTINCT pt.property_type) as property_types,
                        COUNT(DISTINCT fu.feature_name) as features_used
                    FROM tests t
                    JOIN repositories r ON t.repo_id = r.id
                    LEFT JOIN generator_usage gu ON t.id = gu.test_id
                    LEFT JOIN property_types pt ON t.id = pt.test_id
                    LEFT JOIN feature_usage fu ON t.id = fu.test_id
                    WHERE r.owner || '/' || r.name = ?
                    GROUP BY t.id
                """,
                    conn,
                    params=[selected_repo],
                )

                if not test_details.empty:
                    st.subheader(f"Tests in {selected_repo}")

                    # Make GitHub permalinks clickable
                    test_details_display = test_details.copy()

                    st.dataframe(
                        test_details_display,
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "node_id": st.column_config.TextColumn("Test", width="large"),
                            "status": st.column_config.TextColumn("Status", width="small"),
                            "has_source": st.column_config.TextColumn("Source", width="small"),
                            "github_permalink": st.column_config.LinkColumn(
                                "GitHub",
                                width="small",
                                display_text="View"
                            ),
                            "generator_count": st.column_config.NumberColumn("Generators", width="small"),
                            "property_types": st.column_config.NumberColumn("Types", width="small"),
                            "features_used": st.column_config.NumberColumn("Features", width="small"),
                        },
                    )

                    # Add option to view property source
                    if st.checkbox("Show property source code"):
                        selected_test = st.selectbox(
                            "Select test to view source",
                            test_details[test_details["has_source"] == "Yes"]["node_id"].tolist()
                        )

                        if selected_test:
                            with db.connection() as conn:
                                source = conn.execute(
                                    """
                                    SELECT property_text, github_permalink
                                    FROM tests t
                                    JOIN repositories r ON t.repo_id = r.id
                                    WHERE r.owner || '/' || r.name = ?
                                    AND t.node_id = ?
                                    """,
                                    (selected_repo, selected_test),
                                ).fetchone()

                                if source and source["property_text"]:
                                    if source["github_permalink"]:
                                        st.link_button(
                                            "View on GitHub",
                                            source["github_permalink"]
                                        )
                                    st.code(source["property_text"], language="python")


def render_coverage_analysis():
    """Render coverage analysis and visualization."""
    st.header("Test Coverage Analysis")

    db = get_database()

    # Get overall coverage statistics
    with db.connection() as conn:
        # Overall coverage stats
        overall_stats = pd.read_sql_query(
            """
            SELECT
                COUNT(DISTINCT tc.test_id) as tests_with_coverage,
                COUNT(DISTINCT tc.file_path) as files_covered,
                SUM(tc.covered_lines) as total_lines_covered,
                AVG(tc.covered_lines) as avg_lines_per_file
            FROM test_coverage tc
            """,
            conn,
        )

        # Coverage by repository
        repo_coverage = pd.read_sql_query(
            """
            SELECT
                r.owner || '/' || r.name as repository,
                COUNT(DISTINCT t.id) as total_tests,
                COUNT(DISTINCT tc.test_id) as tests_with_coverage,
                SUM(tc.covered_lines) as total_lines_covered,
                COUNT(DISTINCT tc.file_path) as files_covered
            FROM repositories r
            LEFT JOIN tests t ON r.id = t.repo_id
            LEFT JOIN test_coverage tc ON t.id = tc.test_id
            WHERE r.clone_status = 'success'
            GROUP BY r.id
            HAVING tests_with_coverage > 0
            ORDER BY total_lines_covered DESC
            LIMIT 20
            """,
            conn,
        )

        # Coverage over time
        coverage_timeline = pd.read_sql_query(
            """
            SELECT
                DATE(tc.collected_at) as date,
                COUNT(DISTINCT tc.test_id) as tests_count,
                SUM(tc.covered_lines) as lines_covered,
                COUNT(DISTINCT tc.file_path) as files_covered
            FROM test_coverage tc
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
            FROM test_executions
            """,
            conn,
        )

    # Display overall metrics
    col1, col2, col3, col4 = st.columns(4)

    if not overall_stats.empty:
        with col1:
            st.metric(
                "Tests with Coverage",
                f"{overall_stats['tests_with_coverage'].iloc[0]:,}",
            )
        with col2:
            st.metric("Files Covered", f"{overall_stats['files_covered'].iloc[0]:,}")
        with col3:
            lines = overall_stats["total_lines_covered"].iloc[0]
            st.metric(
                "Total Lines Covered",
                f"{lines:,}" if lines else "0",
            )
        with col4:
            avg_lines = overall_stats["avg_lines_per_file"].iloc[0]
            st.metric(
                "Avg Lines/File",
                f"{avg_lines:.0f}" if avg_lines else "0",
            )

    # Coverage by repository chart
    if not repo_coverage.empty:
        st.subheader("Coverage by Repository")

        fig = px.bar(
            repo_coverage.head(15),
            x="total_lines_covered",
            y="repository",
            orientation="h",
            title="Top 15 Repositories by Lines Covered",
            labels={
                "total_lines_covered": "Total Lines Covered",
                "repository": "Repository",
            },
            color="total_lines_covered",
            color_continuous_scale="Viridis",
            hover_data=["total_tests", "tests_with_coverage", "files_covered"],
        )
        fig.update_layout(height=500)
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
                "Total Test Executions", f"{test_results['total_executions'].iloc[0]:,}"
            )
            st.metric("Pass Rate", f"{(passed / (passed + failed) * 100):.1f}%")
            avg_time = test_results["avg_execution_time"].iloc[0]
            st.metric("Avg Execution Time", f"{avg_time:.2f}s" if avg_time else "N/A")

    # Cumulative coverage over test cases
    st.subheader("Cumulative Coverage Growth")

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
            FROM test_case_coverage tcc
            JOIN tests t ON tcc.test_id = t.id
            JOIN repositories r ON t.repo_id = r.id
            ORDER BY t.id, tcc.file_path, tcc.case_number
            """,
            conn,
        )

    if not cumulative_data.empty:
        # Select a test to visualize
        test_options = cumulative_data[["repository", "node_id"]].drop_duplicates()
        test_display = test_options.apply(
            lambda x: f"{x['repository']} - {x['node_id'].split('::')[-1]}", axis=1
        )
        selected_test_idx = st.selectbox(
            "Select a test to view cumulative coverage",
            range(len(test_display)),
            format_func=lambda x: test_display.iloc[x],
        )

        if selected_test_idx is not None:
            selected_test = test_options.iloc[selected_test_idx]
            test_data = cumulative_data[
                (cumulative_data["repository"] == selected_test["repository"])
                & (cumulative_data["node_id"] == selected_test["node_id"])
            ]

            if not test_data.empty:
                # Create cumulative coverage chart
                fig = go.Figure()

                # Add a line for each file
                for file_path in test_data["file_path"].unique():
                    file_data = test_data[test_data["file_path"] == file_path]
                    # Extract just the filename for display
                    display_name = file_path.split("/")[-1]

                    fig.add_trace(
                        go.Scatter(
                            x=file_data["case_number"],
                            y=file_data["cumulative_count"],
                            mode="lines+markers",
                            name=display_name,
                            line={"width": 2},
                        )
                    )

                fig.update_layout(
                    title=f"Cumulative Coverage Growth: {selected_test['node_id'].split('::')[-1]}",
                    xaxis_title="Test Case Number",
                    yaxis_title="Cumulative Lines Covered",
                    hovermode="x unified",
                    height=400,
                )

                st.plotly_chart(fig, use_container_width=True)

                # Show summary statistics
                col1, col2, col3 = st.columns(3)
                with col1:
                    total_cases = test_data["case_number"].max() + 1
                    st.metric("Total Test Cases", total_cases)
                with col2:
                    final_coverage = (
                        test_data.groupby("file_path")["cumulative_count"].max().sum()
                    )
                    st.metric("Final Lines Covered", final_coverage)
                with col3:
                    files_count = test_data["file_path"].nunique()
                    st.metric("Files Touched", files_count)

    # Detailed test coverage view
    st.subheader("Detailed Test Coverage")

    selected_repo = st.selectbox(
        "Select repository for detailed coverage view",
        repo_coverage["repository"].tolist() if not repo_coverage.empty else [],
    )

    if selected_repo:
        with db.connection() as conn:
            # Get test coverage details for selected repository
            test_coverage_details = pd.read_sql_query(
                """
                SELECT
                    t.node_id,
                    tc.file_path,
                    tc.covered_lines,
                    te.passed,
                    te.execution_time
                FROM tests t
                JOIN repositories r ON t.repo_id = r.id
                LEFT JOIN test_coverage tc ON t.id = tc.test_id
                LEFT JOIN test_executions te ON t.id = te.test_id
                WHERE r.owner || '/' || r.name = ?
                AND tc.covered_lines IS NOT NULL
                ORDER BY tc.covered_lines DESC
                """,
                conn,
                params=[selected_repo],
            )

            if not test_coverage_details.empty:
                st.dataframe(
                    test_coverage_details,
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


def render_analysis_history():
    """Render analysis run history."""
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


def main():
    """Main dashboard application."""
    st.title("Property-Based Testing Corpus Analysis")

    # Sidebar
    with st.sidebar:
        st.title("Navigation")
        page = st.radio(
            "Select Page",
            [
                "Overview",
                "Generators",
                "Property Types",
                "Features",
                "Property Explorer",
                "Coverage",
                "Repositories",
                "History",
            ],
        )

        # Refresh button
        if st.button("Refresh Data"):
            st.cache_resource.clear()
            st.rerun()

        # Last update time
        st.markdown("---")
        st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    try:
        stats = load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.info("Make sure the analysis has been run at least once.")
        return

    # Render selected page
    if page == "Overview":
        render_overview_metrics(stats)

        # Quick stats in columns
        col1, col2 = st.columns(2)
        with col1:
            render_generator_analysis(stats)
        with col2:
            render_property_types(stats)

    elif page == "Generators":
        render_generator_analysis(stats)

    elif page == "Property Types":
        render_property_types(stats)

    elif page == "Features":
        render_feature_usage(stats)

    elif page == "Property Explorer":
        render_property_explorer()

    elif page == "Coverage":
        render_coverage_analysis()

    elif page == "Repositories":
        render_repository_details()

    elif page == "History":
        render_analysis_history()


if __name__ == "__main__":
    main()
