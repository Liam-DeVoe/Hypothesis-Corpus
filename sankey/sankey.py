"""
Generate a Sankey diagram showing the repository filtering pipeline.

Usage:
    python sankey/sankey.py [--db-path analysis/data.db]

Pipeline stages match the top-level bullets in DATASET_README.md:
    1. GitHub code search
    2. Filter >1gb
    3. Filter low-star forks
    4. Filter no test files / vendored site-packages
    5. Filter duplicates (MinHash)
    6. Filter unexecutable tests (install + pytest --collect-only)
"""

import argparse
import sqlite3

import plotly.graph_objects as go


def get_counts(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    counts = {}
    counts["total"] = c.execute("SELECT COUNT(*) FROM core_repository").fetchone()[0]
    counts["invalid_repo"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'invalid_repo'"
    ).fetchone()[0]
    counts["minhash_duplicate"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason LIKE 'minhash_duplicate%'"
    ).fetchone()[0]
    counts["minhash_error"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'minhash_error'"
    ).fetchone()[0]
    counts["repo_404"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'repo_404'"
    ).fetchone()[0]
    counts["install_error"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'install_error'"
    ).fetchone()[0]
    counts["no_hypothesis_tests"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'invalid_install (no_hypothesis_tests)'"
    ).fetchone()[0]
    counts["timed_out"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status_reason = 'invalid_install (timed_out)'"
    ).fetchone()[0]
    counts["valid"] = c.execute(
        "SELECT COUNT(*) FROM core_repository WHERE status = 'valid'"
    ).fetchone()[0]

    conn.close()
    return counts


def build_sankey(counts):
    total = counts["total"]
    invalid_repo = counts["invalid_repo"]
    minhash_error = counts["minhash_error"]
    minhash_duplicate = counts["minhash_duplicate"]
    repo_404 = counts["repo_404"]
    install_error = counts["install_error"]
    no_hypothesis_tests = counts["no_hypothesis_tests"]
    timed_out = counts["timed_out"]
    valid = counts["valid"]

    # Derived counts
    into_test_file_filter = total
    into_minhash = into_test_file_filter - invalid_repo
    into_install = into_minhash - minhash_duplicate - minhash_error
    into_final = valid + repo_404

    nodes = []
    node_colors = []

    def add_node(label, color):
        nodes.append(label)
        node_colors.append(color)
        return len(nodes) - 1

    links_source = []
    links_target = []
    links_value = []
    links_color = []

    def add_link(source, target, value, color):
        links_source.append(source)
        links_target.append(target)
        links_value.append(value)
        links_color.append(color)

    GREEN = "#00CC96"
    RED = "#EF553B"
    BLUE = "#636EFA"
    ORANGE = "#FFA15A"
    GRAY = "#AAAAAA"

    LINK_RED = "rgba(239, 85, 59, 0.3)"
    LINK_BLUE = "rgba(99, 110, 250, 0.4)"
    LINK_GRAY = "rgba(170, 170, 170, 0.4)"

    # -- Pipeline step nodes --
    n_github = add_node("GitHub code search<br>(unknown total)", BLUE)
    n_size_filter = add_node("Size filter<br>(unknown count)", GRAY)
    n_fork_filter = add_node("Fork filter<br>(unknown count)", GRAY)
    n_test_file_filter = add_node(
        f"Test file filter<br>{into_test_file_filter:,}", BLUE
    )
    n_minhash = add_node(f"Repository deduplication<br>{into_minhash:,}", BLUE)
    n_install = add_node(f"Test collection<br>{into_install:,}", BLUE)
    n_final = add_node(f"Final corpus<br>{valid:,}", GREEN)

    # -- Rejection nodes --
    n_too_large = add_node("Too large (>1gb)<br>(unknown)", GRAY)
    n_low_star_fork = add_node("Low-star fork<br>(unknown)", GRAY)
    n_no_test_files = add_node(
        f"No test files or<br>vendored site-packages<br>{invalid_repo:,}", RED
    )
    n_minhash_dup = add_node(f"Duplicate<br>{minhash_duplicate:,}", RED)
    n_minhash_err = add_node(f"MinHash error<br>{minhash_error:,}", RED)
    n_no_hypothesis = add_node(f"No Hypothesis tests<br>{no_hypothesis_tests:,}", RED)
    n_install_error = add_node(f"Install error<br>{install_error:,}", RED)
    n_timed_out = add_node(f"Collection timed out<br>{timed_out:,}", RED)
    n_repo_404 = add_node(f"Repo later deleted<br>from GitHub<br>{repo_404:,}", ORANGE)

    # -- Links --

    # GitHub search → size filter (all repos), plus unknown rejection
    add_link(n_github, n_size_filter, total, LINK_BLUE)
    add_link(n_github, n_too_large, 1, LINK_GRAY)

    # Size filter → fork filter, plus unknown rejection
    add_link(n_size_filter, n_fork_filter, total, LINK_BLUE)
    add_link(n_size_filter, n_low_star_fork, 1, LINK_GRAY)

    # Fork filter → test file filter
    add_link(n_fork_filter, n_test_file_filter, into_test_file_filter, LINK_BLUE)

    # Test file filter → minhash + rejection
    add_link(n_test_file_filter, n_no_test_files, invalid_repo, LINK_RED)
    add_link(n_test_file_filter, n_minhash, into_minhash, LINK_BLUE)

    # MinHash → install + rejections
    add_link(n_minhash, n_minhash_dup, minhash_duplicate, LINK_RED)
    add_link(n_minhash, n_minhash_err, minhash_error, LINK_RED)
    add_link(n_minhash, n_install, into_install, LINK_BLUE)

    # Test collection → final + rejections
    add_link(n_install, n_no_hypothesis, no_hypothesis_tests, LINK_RED)
    add_link(n_install, n_install_error, install_error, LINK_RED)
    add_link(n_install, n_timed_out, timed_out, LINK_RED)
    add_link(n_install, n_final, into_final, LINK_BLUE)

    # Final corpus → repo 404
    add_link(n_final, n_repo_404, repo_404, LINK_GRAY)

    fig = go.Figure(
        data=[
            go.Sankey(
                node={
                    "pad": 20,
                    "thickness": 25,
                    "line": {"color": "white", "width": 2},
                    "label": nodes,
                    "color": node_colors,
                },
                link={
                    "source": links_source,
                    "target": links_target,
                    "value": links_value,
                    "color": links_color,
                },
            )
        ]
    )

    fig.update_layout(
        title_text="Repository Filtering Pipeline",
        font_size=12,
        width=1200,
        height=700,
    )

    return fig


def main():
    parser = argparse.ArgumentParser(
        description="Generate repository filtering Sankey diagram"
    )
    parser.add_argument(
        "--db-path", default="analysis/data.db", help="Path to database file"
    )
    parser.add_argument(
        "--output", default="sankey/output.html", help="Output HTML file path"
    )
    args = parser.parse_args()

    counts = get_counts(args.db_path)
    fig = build_sankey(counts)
    fig.write_html(args.output)
    print(f"Saved to {args.output}")

if __name__ == "__main__":
    main()
