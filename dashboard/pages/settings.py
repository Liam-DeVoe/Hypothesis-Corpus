import json
import re
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dashboard.shared import histogram_with_kde
from dashboard.utils import get_database, render_sidebar

st.set_page_config(
    page_title="Settings",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)
db = get_database()


def max_examples_histogram(db):
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


def deadline_histogram(db):
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


def stateful_step_count_histogram(db):
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


def derandomize_bar_chart(db):
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


def backend_bar_chart(db):
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

    settings_data["backend"] = settings_data["backend"].fillna("")
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


def database_bar_chart(db):
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

    def clean_database_name(db_str):
        if db_str is None or db_str == "<class 'NoneType'>":
            return "None"
        match = re.match(r"<class '(.+?)'>", str(db_str))
        if match:
            full_name = match.group(1)
            return full_name.split(".")[-1]
        return str(db_str)

    settings_data["database_label"] = settings_data["database"].apply(
        clean_database_name
    )
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


def suppress_health_check_bar_chart(db):
    HEALTH_CHECK_NAMES = {
        1: "data_too_large",
        2: "filter_too_much",
        3: "too_slow",
        7: "large_base_example",
        9: "function_scoped_fixture",
        10: "differing_executors",
        11: "nested_given",
    }

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

    health_check_counts = {}

    for shc_json in settings_data["suppress_health_check"]:
        shc_list = json.loads(shc_json) if isinstance(shc_json, str) else shc_json
        for hc_value in shc_list:
            hc_name = HEALTH_CHECK_NAMES.get(hc_value, f"unknown_{hc_value}")
            health_check_counts[hc_name] = health_check_counts.get(hc_name, 0) + 1

    if not health_check_counts:
        return None, empty_count, total

    sorted_items = sorted(health_check_counts.items(), key=lambda x: x[1], reverse=True)
    labels = [item[0] for item in sorted_items]
    counts_list = [item[1] for item in sorted_items]

    fig = go.Figure(
        data=[
            go.Bar(
                x=labels,
                y=counts_list,
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
    render_sidebar()

    st.header("Settings")

    db = get_database()

    fig = max_examples_histogram(db)
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No max_examples data available.")

    result = deadline_histogram(db)
    if result and result[0]:
        fig, null_count, total = result
        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(fig, width="stretch")
        with col2:
            if total > 0:
                st.markdown("**deadline = None**")
                st.markdown(f"{null_count:,} / {total:,}")
    else:
        st.info("No deadline data available.")

    fig = stateful_step_count_histogram(db)
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No stateful_step_count data available.")

    fig = derandomize_bar_chart(db)
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No derandomize data available.")

    fig = backend_bar_chart(db)
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No backend data available.")

    fig = database_bar_chart(db)
    if fig:
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No database data available.")

    result = suppress_health_check_bar_chart(db)
    if result and result[0]:
        fig, empty_count, total = result
        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(fig, width="stretch")
        with col2:
            if total > 0:
                st.markdown("**suppress_health_check = []**")
                st.markdown(f"{empty_count:,} / {total:,}")
    else:
        st.info("No suppress_health_check data available.")


if __name__ == "__main__":
    main()
