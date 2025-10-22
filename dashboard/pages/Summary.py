import json
import re
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import execution_frequency_histogram, histogram_with_kde
from dashboard.utils import get_database, render_sidebar

# Page configuration
st.set_page_config(
    page_title="Summary",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)
db = get_database()


def hypothesis_test_count_histogram():
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


def unique_test_count_histogram():
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

    # Count unique test functions (strip parametrization)
    unique_counts = []
    for _, row in repo_counts.iterrows():
        node_ids = json.loads(row["node_ids"])
        # Strip parametrization by removing everything after the first '[' if present
        unique_tests = set()
        for node_id in node_ids:
            # Remove parametrization: "test.py::test_foo[param]" -> "test.py::test_foo"
            base_test = node_id.split("[")[0]
            unique_tests.add(base_test)
        unique_counts.append(len(unique_tests))

    return histogram_with_kde(
        data=unique_counts,
        title="Hypothesis test count by repository (grouping @pytest.mark.parametrize)",
        xaxis_title="# of Hypothesis tests (grouping @pytest.mark.parametrize)",
        yaxis_title="Repository count",
        bin_size=1,
        x_type="log",
    )


def hypothesis_percentage_histogram():
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


def timing_histogram():
    execution_times = pd.read_sql_query(
        """
        SELECT execution_time
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )

    if execution_times.empty or len(execution_times) == 0:
        return None

    return histogram_with_kde(
        data=execution_times["execution_time"].tolist(),
        title="Tests by execution time",
        xaxis_title="Execution time (seconds)",
        yaxis_title="Test count",
        bin_size=0.01,
    )


def max_examples_histogram():
    settings_data = pd.read_sql_query(
        """
        SELECT json_extract(settings, '$.max_examples') as max_examples
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )
    if settings_data.empty:
        return None

    return histogram_with_kde(
        data=settings_data["max_examples"].tolist(),
        title="Distribution of max_examples",
        xaxis_title="max_examples",
        yaxis_title="Test count",
        bin_size=10,
    )


def deadline_histogram():
    # Get counts of NULL deadlines and total
    counts = pd.read_sql_query(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN json_extract(settings, '$.deadline') IS NULL THEN 1 ELSE 0 END) as null_count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )

    total = counts["total"].iloc[0] if not counts.empty else 0
    null_count = counts["null_count"].iloc[0] if not counts.empty else 0

    settings_data = pd.read_sql_query(
        """
        SELECT json_extract(settings, '$.deadline') as deadline
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        AND json_extract(settings, '$.deadline') IS NOT NULL
        """,
        db._conn,
    )

    if settings_data.empty:
        return None, null_count, total

    fig = histogram_with_kde(
        data=settings_data["deadline"].tolist(),
        title="Distribution of deadline",
        xaxis_title="deadline (seconds)",
        yaxis_title="Test count",
        bin_size=0.05,
    )

    return fig, null_count, total


def stateful_step_count_histogram():
    settings_data = pd.read_sql_query(
        """
        SELECT json_extract(settings, '$.stateful_step_count') as stateful_step_count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )
    if settings_data.empty:
        return None

    return histogram_with_kde(
        data=settings_data["stateful_step_count"].tolist(),
        title="Distribution of stateful_step_count",
        xaxis_title="stateful_step_count",
        yaxis_title="Test count",
        bin_size=5,
    )


def derandomize_bar_chart():
    settings_data = pd.read_sql_query(
        """
        SELECT
            json_extract(settings, '$.derandomize') as derandomize,
            COUNT(*) as count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        GROUP BY derandomize
        """,
        db._conn,
    )
    if settings_data.empty:
        return None

    settings_data["derandomize_label"] = settings_data["derandomize"].apply(
        lambda x: "True" if x else "False"
    )

    fig = go.Figure(
        data=[
            go.Bar(
                x=settings_data["derandomize_label"],
                y=settings_data["count"],
                marker_color="steelblue",
            )
        ]
    )

    fig.update_layout(
        title="Distribution of derandomize",
        xaxis_title="derandomize",
        yaxis_title="Test count",
        height=400,
        showlegend=False,
    )

    return fig


def median_choices_size_histogram():
    choices_data = pd.read_sql_query(
        """
        SELECT node_id, choices_size
        FROM runtime_testcase
        """,
        db._conn,
    )
    if choices_data.empty:
        return None

    median_per_node = choices_data.groupby("node_id")["choices_size"].median()
    if median_per_node.empty:
        return None

    return histogram_with_kde(
        data=median_per_node.tolist(),
        title="Median choices_size by test",
        xaxis_title="Median choices_size",
        yaxis_title="Test count",
        bin_size=5,
    )


def backend_bar_chart():
    """Bar chart showing distribution of backend values."""
    settings_data = pd.read_sql_query(
        """
        SELECT
            json_extract(settings, '$.backend') as backend,
            COUNT(*) as count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        GROUP BY backend
        """,
        db._conn,
    )

    if settings_data.empty:
        return None

    # Replace NULL with empty string for display
    settings_data["backend"] = settings_data["backend"].fillna("")

    # Sort by count descending
    settings_data = settings_data.sort_values("count", ascending=False)

    fig = go.Figure(
        data=[
            go.Bar(
                x=settings_data["backend"],
                y=settings_data["count"],
                marker_color="steelblue",
            )
        ]
    )

    fig.update_layout(
        title="Distribution of backend",
        xaxis_title="Backend",
        yaxis_title="Test count",
        height=400,
        showlegend=False,
    )

    return fig


def database_bar_chart():
    """Bar chart showing distribution of database types."""
    settings_data = pd.read_sql_query(
        """
        SELECT
            json_extract(settings, '$.database') as database,
            COUNT(*) as count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        GROUP BY database
        """,
        db._conn,
    )
    if settings_data.empty:
        return None

    # Clean up class names to be more readable
    def clean_database_name(db_str):
        if db_str is None or db_str == "<class 'NoneType'>":
            return "None"
        # Extract content from <class 'content'> using regex
        match = re.match(r"<class '(.+?)'>", str(db_str))
        if match:
            full_name = match.group(1)
            # Get just the class name after the last dot
            return full_name.split(".")[-1]
        return str(db_str)

    settings_data["database_label"] = settings_data["database"].apply(clean_database_name)

    # Sort by count descending
    settings_data = settings_data.sort_values("count", ascending=False)

    fig = go.Figure(
        data=[
            go.Bar(
                x=settings_data["database_label"],
                y=settings_data["count"],
                marker_color="steelblue",
            )
        ]
    )

    fig.update_layout(
        title="Distribution of database",
        xaxis_title="Database type",
        yaxis_title="Test count",
        height=400,
        showlegend=False,
    )

    return fig


def suppress_health_check_bar_chart():
    """Bar chart showing distribution of suppress_health_check values."""
    # HealthCheck enum mapping from hypothesis
    HEALTH_CHECK_NAMES = {
        1: "data_too_large",
        2: "filter_too_much",
        3: "too_slow",
        7: "large_base_example",
        9: "function_scoped_fixture",
        10: "differing_executors",
        11: "nested_given",
    }

    # Get counts of empty lists and total
    counts = pd.read_sql_query(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE
                WHEN json_extract(settings, '$.suppress_health_check') = '[]'
                THEN 1 ELSE 0
            END) as empty_count
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        """,
        db._conn,
    )

    total = counts["total"].iloc[0] if not counts.empty else 0
    empty_count = counts["empty_count"].iloc[0] if not counts.empty else 0

    settings_data = pd.read_sql_query(
        """
        SELECT json_extract(settings, '$.suppress_health_check') as suppress_health_check
        FROM runtime_summary
        WHERE status IN ('passed', 'failed')
        AND json_extract(settings, '$.suppress_health_check') IS NOT NULL
        AND json_extract(settings, '$.suppress_health_check') != '[]'
        """,
        db._conn,
    )

    if settings_data.empty:
        return None, empty_count, total

    # Count occurrences of each health check
    health_check_counts = {}

    for shc_json in settings_data["suppress_health_check"]:
        shc_list = json.loads(shc_json) if isinstance(shc_json, str) else shc_json
        for hc_value in shc_list:
            hc_name = HEALTH_CHECK_NAMES.get(hc_value, f"unknown_{hc_value}")
            health_check_counts[hc_name] = health_check_counts.get(hc_name, 0) + 1

    if not health_check_counts:
        return None, empty_count, total

    # Sort by count descending
    sorted_items = sorted(health_check_counts.items(), key=lambda x: x[1], reverse=True)
    labels = [item[0] for item in sorted_items]
    counts = [item[1] for item in sorted_items]

    fig = go.Figure(
        data=[
            go.Bar(
                x=labels,
                y=counts,
                marker_color="steelblue",
            )
        ]
    )

    fig.update_layout(
        title="Distribution of suppress_health_check",
        xaxis_title="Health check",
        yaxis_title="Test count",
        height=400,
        showlegend=False,
    )

    return fig, empty_count, total


def main():
    """Summary page with key research findings."""
    # Sidebar
    render_sidebar()

    st.header("Summary")

    fig = hypothesis_test_count_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No Hypothesis test data available.")

    fig = unique_test_count_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No test data available.")

    fig = hypothesis_percentage_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No test data available.")

    fig = timing_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No execution time data available.")

    fig = execution_frequency_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No line execution frequency data available.")

    fig = max_examples_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No max_examples data available.")

    result = deadline_histogram()
    if result and result[0]:
        fig, null_count, total = result

        col1, col2 = st.columns([3, 1])

        with col1:
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if total > 0:
                st.markdown("**deadline = None**")
                st.markdown(f"{null_count:,} / {total:,}")
    else:
        st.info("No deadline data available.")

    fig = stateful_step_count_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No stateful_step_count data available.")

    fig = derandomize_bar_chart()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No derandomize data available.")

    fig = backend_bar_chart()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No backend data available.")

    fig = database_bar_chart()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No database data available.")

    fig = median_choices_size_histogram()
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No choices_size data available.")

    result = suppress_health_check_bar_chart()
    if result and result[0]:
        fig, empty_count, total = result

        col1, col2 = st.columns([3, 1])

        with col1:
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if total > 0:
                st.markdown("**suppress_health_check = []**")
                st.markdown(f"{empty_count:,} / {total:,}")
    else:
        st.info("No suppress_health_check data available.")


if __name__ == "__main__":
    main()
