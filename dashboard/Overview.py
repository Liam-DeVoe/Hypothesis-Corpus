import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# Add parent directory to path so we can import analysis
sys.path.insert(0, str(Path(__file__).parent.parent))

from dashboard.utils import get_database, render_sidebar

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
    /* Green progress bars in dataframes */
    [data-testid="stDataFrame"] [role="progressbar"] > div,
    [data-testid="stDataFrame"] [data-testid="stDataFrameGlideDataEditor"] [role="progressbar"] > div,
    [data-testid="column-_runtime_percent"] [role="progressbar"] > div,
    [data-testid="column-_facets_percent"] [role="progressbar"] > div,
    .stDataFrame [role="progressbar"] > div {
        background-color: #4CAF50 !important;
        background: #4CAF50 !important;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Allow cmd+click (Mac) / ctrl+click (Windows/Linux) on sidebar nav links
# to open pages in a new tab. Streamlit's navigation intercepts clicks and
# does client-side routing, which prevents the default browser behavior.
# Uses event delegation on the parent document so it survives DOM re-renders,
# with a guard flag on the parent window to prevent duplicate listeners.
components.html(
    """
<script>
(function() {
    const win = window.parent;
    const doc = win.document;
    if (win._sidebarNewTabSetup) return;
    win._sidebarNewTabSetup = true;
    doc.addEventListener('click', function(e) {
        if (!(e.metaKey || e.ctrlKey)) return;
        const link = e.target.closest('a');
        if (!link) return;
        if (!link.closest('[data-testid="stSidebarNav"]')) return;
        e.preventDefault();
        e.stopPropagation();
        win.open(link.href, '_blank');
    }, true);
})();
</script>
""",
    height=0,
)


def load_data():
    """Load data from database."""
    db = get_database()

    repo_stats = db.fetchone(
        """
        SELECT
            COUNT(*) as total
        FROM core_repository
        WHERE status = 'valid'
        """
    )

    node_stats = db.fetchone(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN cn.canonical_parametrization THEN 1 ELSE 0 END) as canonical,
            SUM(CASE WHEN rs.id IS NOT NULL THEN 1 ELSE 0 END) as analyzed,
            SUM(CASE WHEN rs.status = 'passed' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN rs.status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM core_node cn
        JOIN core_repository r ON cn.repo_id = r.id
        LEFT JOIN runtime_summary rs ON cn.id = rs.node_id
        WHERE r.status = 'valid'
        """
    )

    return {
        "repositories": dict(repo_stats),
        "core_node": dict(node_stats),
    }


def load_experiment_progress():
    """Load experiment progress data from database."""
    db = get_database()

    runtime = db.fetchone(
        """
        SELECT
            COUNT(DISTINCT cn.id) as total_nodes,
            COUNT(DISTINCT rs.node_id) as processed,
            SUM(CASE WHEN rs.status = 'passed' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN rs.status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN rs.status = 'skipped' THEN 1 ELSE 0 END) as skipped,
            SUM(CASE WHEN rs.status = 'error' THEN 1 ELSE 0 END) as error
        FROM core_node cn
        JOIN core_repository r ON cn.repo_id = r.id
        LEFT JOIN runtime_summary rs ON cn.id = rs.node_id
        WHERE r.status = 'valid'
    """
    )

    facets = db.fetchone(
        """
        SELECT
            (SELECT COUNT(*) FROM core_node cn
             JOIN core_repository r ON cn.repo_id = r.id
             WHERE cn.canonical_parametrization = TRUE AND r.status = 'valid') as total_canonical,
            COUNT(DISTINCT CASE WHEN fn.type = 'summary' THEN fn.node_id END) as with_summary,
            COUNT(DISTINCT CASE WHEN fn.type = 'pattern' THEN fn.node_id END) as with_pattern,
            COUNT(DISTINCT CASE WHEN fn.type = 'domain' THEN fn.node_id END) as with_domain
        FROM facets_nodes fn
        JOIN core_node cn ON fn.node_id = cn.id
        JOIN core_repository r ON cn.repo_id = r.id
        WHERE cn.canonical_parametrization = TRUE AND r.status = 'valid'
    """
    )

    # Repository-level progress
    # Note: Use COUNT(DISTINCT) for canonical_nodes to avoid inflation from JOINs
    repos = db.fetchall(
        """
        SELECT
            r.full_name,
            COUNT(DISTINCT cn.id) as total_nodes,
            COUNT(DISTINCT CASE WHEN cn.canonical_parametrization THEN cn.id END) as canonical_nodes,
            COUNT(DISTINCT rs.node_id) as runtime_done,
            COUNT(DISTINCT CASE WHEN rs.status = 'passed' THEN rs.node_id END) as runtime_passed,
            COUNT(DISTINCT CASE WHEN rs.status = 'failed' THEN rs.node_id END) as runtime_failed,
            COUNT(DISTINCT CASE WHEN rs.status = 'skipped' THEN rs.node_id END) as runtime_skipped,
            COUNT(DISTINCT CASE WHEN rs.status = 'error' THEN rs.node_id END) as runtime_error,
            COUNT(DISTINCT CASE WHEN fn.type = 'summary' THEN fn.node_id END) as facets_done
        FROM core_repository r
        JOIN core_node cn ON r.id = cn.repo_id
        LEFT JOIN runtime_summary rs ON cn.id = rs.node_id
        LEFT JOIN facets_nodes fn ON cn.id = fn.node_id
        WHERE r.status = 'valid'
        GROUP BY r.id
        ORDER BY r.full_name
    """
    )

    return {
        "runtime": dict(runtime) if runtime else {},
        "facets": dict(facets) if facets else {},
        "repos": [dict(r) for r in repos] if repos else [],
    }


def render_overview_metrics(stats: dict[str, Any]):
    """Render overview metrics."""
    st.header("Overview")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Repositories",
            f"{stats['repositories']['total']:,}",
        )

    with col2:
        total = stats["core_node"]["total"] or 0
        st.metric(
            "All Nodes",
            f"{total:,}",
        )

    with col3:
        canonical = stats["core_node"]["canonical"] or 0
        st.metric(
            "Canonical Nodes",
            f"{canonical:,}",
        )


def render_repo_progress_table(
    repos: list[dict[str, Any]],
    experiments: list[dict[str, str]],
    *,
    expander_title: str,
    expanded: bool = False,
):
    """Render a repository progress table for one or more experiments.

    Args:
        repos: List of repository dicts with progress data
        experiments: List of experiment configs, each with:
            - label: Display name (e.g., "Runtime")
            - done_key: Key in repo dict for completed count (e.g., "runtime_done")
            - total_key: Key in repo dict for total count (e.g., "total_nodes")
        expander_title: Title for the expander
        expanded: Whether expander starts expanded
    """
    if not repos:
        return

    with st.expander(expander_title, expanded=expanded):
        # Build DataFrame rows
        rows = []
        for r in repos:
            row = {"Repository": r["full_name"]}
            for exp in experiments:
                label = exp["label"]
                done = r[exp["done_key"]]
                total = r[exp["total_key"]]
                percent = done / total * 100 if total else 0
                row[f"_{label.lower()}_percent"] = percent
                row[label] = f"{done} / {total}"
            rows.append(row)

        df = pd.DataFrame(rows)

        # Build column order and config
        column_order = ["Repository"]
        column_config = {}
        for exp in experiments:
            label = exp["label"]
            percent_col = f"_{label.lower()}_percent"
            column_order.extend([percent_col, label])
            column_config[percent_col] = st.column_config.ProgressColumn(
                label=label,
                min_value=0,
                max_value=100,
                format="%.0f%%",
            )
            column_config[label] = st.column_config.TextColumn(label="")

        st.dataframe(
            df,
            hide_index=True,
            column_order=column_order,
            column_config=column_config,
        )


def render_experiment_progress(progress: dict[str, Any]):
    """Render experiment progress section."""
    st.subheader("Experiment Progress")

    runtime = progress["runtime"]
    facets = progress["facets"]
    repos = progress["repos"]

    # Handle empty state
    if not runtime.get("total_nodes"):
        st.info("No nodes collected yet. Run `python run.py install` first.")
        return

    # Runtime experiment
    rt_total = runtime["total_nodes"]
    rt_done = runtime["processed"] or 0
    rt_percent = rt_done / rt_total if rt_total > 0 else 0

    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("**Runtime** (all nodes)")
        st.progress(rt_percent)
    with col2:
        st.markdown(f"**{rt_done:,}** / {rt_total:,}")

    if rt_done > 0:
        c1, c2, c3, c4 = st.columns(4)
        c1.caption(f"Passed: {runtime['passed'] or 0:,}")
        c2.caption(f"Failed: {runtime['failed'] or 0:,}")
        c3.caption(f"Skipped: {runtime['skipped'] or 0:,}")
        c4.caption(f"Error: {runtime['error'] or 0:,}")

    # Runtime per-repository table
    render_repo_progress_table(
        repos,
        experiments=[
            {
                "label": "Completed",
                "done_key": "runtime_done",
                "total_key": "total_nodes",
            },
            {
                "label": "Passed",
                "done_key": "runtime_passed",
                "total_key": "total_nodes",
            },
            {
                "label": "Failed",
                "done_key": "runtime_failed",
                "total_key": "total_nodes",
            },
            {
                "label": "Skipped",
                "done_key": "runtime_skipped",
                "total_key": "total_nodes",
            },
            {"label": "Error", "done_key": "runtime_error", "total_key": "total_nodes"},
        ],
        expander_title="Details",
    )

    st.divider()

    # Facets experiment
    fc_total = facets.get("total_canonical") or 0
    fc_done = facets.get("with_summary") or 0
    fc_percent = fc_done / fc_total if fc_total > 0 else 0

    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown("**Facets** (canonical nodes only)")
        st.progress(fc_percent)
    with col2:
        st.markdown(f"**{fc_done:,}** / {fc_total:,}")

    if fc_done > 0:
        c1, c2, c3 = st.columns(3)
        c1.caption(f"With summary: {facets.get('with_summary') or 0:,}")
        c2.caption(f"With patterns: {facets.get('with_pattern') or 0:,}")
        c3.caption(f"With domains: {facets.get('with_domain') or 0:,}")

    # Facets per-repository table
    render_repo_progress_table(
        repos,
        experiments=[
            {
                "label": "Completed",
                "done_key": "facets_done",
                "total_key": "canonical_nodes",
            },
        ],
        expander_title="Details",
    )


def overview_page():
    """Overview page content."""
    # Sidebar
    render_sidebar()

    # Load data
    try:
        stats = load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.info("Make sure the analysis has been run at least once.")
        return

    # Render overview page
    render_overview_metrics(stats)

    # Load experiment progress once for both sections
    try:
        progress = load_experiment_progress()
    except Exception as e:
        st.warning(f"Could not load experiment progress: {e}")
        return

    # Repository details (right below overview) - shows both experiments
    render_repo_progress_table(
        progress["repos"],
        experiments=[
            {
                "label": "Runtime",
                "done_key": "runtime_done",
                "total_key": "total_nodes",
            },
            {
                "label": "Facets",
                "done_key": "facets_done",
                "total_key": "canonical_nodes",
            },
        ],
        expander_title="Details",
    )

    st.divider()

    # Experiment progress section
    render_experiment_progress(progress)


def main():
    """Main dashboard application with custom navigation."""
    pg = st.navigation(
        [
            st.Page(overview_page, title="Overview"),
            st.Page("pages/corpus.py", title="Corpus"),
            st.Page("pages/settings.py", title="Settings"),
            st.Page("pages/timing.py", title="Timing"),
            st.Page("pages/testcases.py", title="Test Cases"),
            st.Page("pages/coverage.py", title="Coverage"),
            st.Page("pages/Facets.py", title="Facets"),
            st.Page("pages/Clusters.py", title="Clusters"),
        ]
    )
    pg.run()


if __name__ == "__main__":
    main()
