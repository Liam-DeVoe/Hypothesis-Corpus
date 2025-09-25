"""
Streamlit dashboard for visualizing PBT corpus analysis results.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import json
from typing import Dict, List, Any

from analyzer.database import Database

# Page configuration
st.set_page_config(
    page_title="PBT Corpus Analysis Dashboard",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
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
""", unsafe_allow_html=True)


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
            stats['repositories']['total'],
            f"{stats['repositories']['successful']} successful"
        )

    with col2:
        st.metric(
            "Total Tests Analyzed",
            stats['tests']['total'],
            f"{stats['tests']['successful']} successful"
        )

    with col3:
        success_rate = (
            stats['repositories']['successful'] / stats['repositories']['total'] * 100
            if stats['repositories']['total'] > 0 else 0
        )
        st.metric(
            "Success Rate",
            f"{success_rate:.1f}%",
            f"{stats['repositories']['failed']} failed"
        )

    with col4:
        pending = stats['repositories']['pending']
        st.metric(
            "Pending",
            pending,
            "In progress" if pending > 0 else "Complete"
        )

    # Progress bar
    if stats['repositories']['total'] > 0:
        progress = (stats['repositories']['successful'] + stats['repositories']['failed']) / stats['repositories']['total']
        st.progress(progress, text=f"Analysis Progress: {progress*100:.1f}%")


def render_generator_analysis(stats: Dict[str, Any]):
    """Render generator usage analysis."""
    st.header("Generator Usage Analysis")

    if not stats.get('top_generators'):
        st.info("No generator data available yet.")
        return

    # Create DataFrame
    df = pd.DataFrame(stats['top_generators'])

    col1, col2 = st.columns([2, 1])

    with col1:
        # Bar chart of top generators
        fig = px.bar(
            df.head(15),
            x='total_uses',
            y='generator_name',
            orientation='h',
            title="Top 15 Most Used Generators",
            labels={'total_uses': 'Total Uses', 'generator_name': 'Generator'},
            color='total_uses',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Statistics
        st.subheader("Generator Statistics")

        total_generators = len(df)
        total_uses = df['total_uses'].sum()
        avg_uses = df['total_uses'].mean()

        st.metric("Unique Generators", total_generators)
        st.metric("Total Generator Uses", f"{total_uses:,}")
        st.metric("Average Uses per Generator", f"{avg_uses:.1f}")

        # Top 5 table
        st.subheader("Top 5 Generators")
        top_5 = df.head(5)[['generator_name', 'total_uses']]
        st.dataframe(top_5, hide_index=True)


def render_property_types(stats: Dict[str, Any]):
    """Render property type distribution."""
    st.header("Property Type Distribution")

    if not stats.get('property_types'):
        st.info("No property type data available yet.")
        return

    df = pd.DataFrame(stats['property_types'])

    col1, col2 = st.columns(2)

    with col1:
        # Pie chart
        fig = px.pie(
            df,
            values='count',
            names='property_type',
            title="Property Type Distribution",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Bar chart
        fig = px.bar(
            df,
            x='property_type',
            y='count',
            title="Property Types by Count",
            labels={'count': 'Number of Tests', 'property_type': 'Property Type'},
            color='count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)


def render_feature_usage(stats: Dict[str, Any]):
    """Render feature usage analysis."""
    st.header("Hypothesis Feature Usage")

    if not stats.get('feature_usage'):
        st.info("No feature usage data available yet.")
        return

    df = pd.DataFrame(stats['feature_usage'])

    # Create a more detailed view
    fig = go.Figure()

    # Add bars for test count
    fig.add_trace(go.Bar(
        name='Tests Using Feature',
        x=df['feature_name'],
        y=df['test_count'],
        yaxis='y',
        marker_color='lightblue'
    ))

    # Add line for total uses
    fig.add_trace(go.Scatter(
        name='Total Feature Uses',
        x=df['feature_name'],
        y=df['total_uses'],
        yaxis='y2',
        mode='lines+markers',
        marker_color='red',
        line=dict(width=2)
    ))

    # Update layout with dual y-axes
    fig.update_layout(
        title="Feature Usage Analysis",
        xaxis_title="Feature",
        yaxis=dict(title="Number of Tests", side='left'),
        yaxis2=dict(title="Total Uses", overlaying='y', side='right'),
        hovermode='x unified',
        height=400
    )

    st.plotly_chart(fig, use_container_width=True)

    # Feature usage table
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Feature Usage Details")
        feature_df = df[['feature_name', 'test_count', 'total_uses']]
        feature_df.columns = ['Feature', 'Tests', 'Total Uses']
        st.dataframe(feature_df, hide_index=True)

    with col2:
        st.subheader("Feature Insights")

        if len(df) > 0:
            most_used = df.loc[df['total_uses'].idxmax()]
            most_tests = df.loc[df['test_count'].idxmax()]

            st.info(f"Most Used Feature: {most_used['feature_name']} ({most_used['total_uses']} uses)")
            st.info(f"Most Popular Feature: {most_tests['feature_name']} (in {most_tests['test_count']} tests)")

            # Calculate average uses per test
            df['avg_uses_per_test'] = df['total_uses'] / df['test_count']
            most_intensive = df.loc[df['avg_uses_per_test'].idxmax()]
            st.info(f"Most Intensive Usage: {most_intensive['feature_name']} ({most_intensive['avg_uses_per_test']:.1f} uses/test)")


def render_repository_details():
    """Render detailed repository analysis."""
    st.header("Repository Details")

    db = get_database()

    # Get repository list
    with db.connection() as conn:
        repos = pd.read_sql_query("""
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
        """, conn)

    if repos.empty:
        st.info("No repositories processed yet.")
        return

    # Filter by status
    status_filter = st.selectbox(
        "Filter by Status",
        ["All", "success", "failed", "pending"],
        index=0
    )

    if status_filter != "All":
        repos = repos[repos['status'] == status_filter]

    # Display repository table
    st.dataframe(
        repos,
        use_container_width=True,
        hide_index=True,
        column_config={
            "repository": st.column_config.TextColumn("Repository", width="medium"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "test_count": st.column_config.NumberColumn("Tests", width="small"),
            "created_at": st.column_config.DatetimeColumn("Analyzed At", width="medium")
        }
    )

    # Repository selection for detailed view
    if not repos.empty:
        selected_repo = st.selectbox("Select repository for detailed view", repos['repository'].tolist())

        if selected_repo:
            with db.connection() as conn:
                # Get test details for selected repository
                test_details = pd.read_sql_query("""
                    SELECT
                        t.node_id,
                        t.status,
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
                """, conn, params=[selected_repo])

                if not test_details.empty:
                    st.subheader(f"Tests in {selected_repo}")
                    st.dataframe(test_details, use_container_width=True, hide_index=True)


def render_analysis_history():
    """Render analysis run history."""
    st.header("Analysis History")

    db = get_database()

    with db.connection() as conn:
        runs = pd.read_sql_query("""
            SELECT * FROM analysis_runs
            ORDER BY start_time DESC
            LIMIT 10
        """, conn)

    if runs.empty:
        st.info("No analysis runs recorded yet.")
        return

    st.dataframe(runs, use_container_width=True, hide_index=True)


def main():
    """Main dashboard application."""
    st.title("Property-Based Testing Corpus Analysis")

    # Sidebar
    with st.sidebar:
        st.title("Navigation")
        page = st.radio(
            "Select Page",
            ["Overview", "Generators", "Property Types", "Features", "Repositories", "History"]
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

    elif page == "Repositories":
        render_repository_details()

    elif page == "History":
        render_analysis_history()


if __name__ == "__main__":
    main()
