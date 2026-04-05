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
    into_collection = into_install - install_error
    into_final = valid + repo_404

    nodes = []
    node_colors = []

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

    # Node positions: x goes left-to-right, y goes top-to-bottom (0=top, 1=bottom).
    # Pipeline steps run along the top, rejections branch downward.
    node_x = []
    node_y = []

    def add_node(label, color, x, y):
        nodes.append(label)
        node_colors.append(color)
        node_x.append(x)
        node_y.append(y)
        return len(nodes) - 1

    # X columns for pipeline steps
    X_GITHUB = 0.001
    X_SIZE = 0.13
    X_FORK = 0.25
    X_TESTFILE = 0.37
    X_MINHASH = 0.50
    X_INSTALL = 0.62
    X_COLLECTION = 0.74
    X_FINAL = 0.87

    # Rejections are offset slightly right of their source step
    X_OFF = 0.07

    # Y positions
    Y_PIPELINE = 0.001       # all pipeline steps at same level
    Y_REJ = 0.55             # all rejections start at same level
    Y_REJ_STACK = 0.15       # vertical spacing between stacked rejections

    # -- Pipeline step nodes (all at Y_PIPELINE) --
    n_github = add_node("GitHub search<br>(unknown count)", BLUE, X_GITHUB, Y_PIPELINE)
    n_size_filter = add_node("Filter large repositories<br>(unknown count)", GRAY, X_SIZE, Y_PIPELINE)
    n_fork_filter = add_node("Filter unpopular forks<br>(unknown count)", GRAY, X_FORK, Y_PIPELINE)
    n_test_file_filter = add_node(
        f"Filter extremely<br>unlikely repositories<br>{into_test_file_filter:,}", BLUE, X_TESTFILE, Y_PIPELINE
    )
    n_minhash = add_node(f"Filter duplicate<br>repositories<br>{into_minhash:,}", BLUE, X_MINHASH, Y_PIPELINE)
    n_install = add_node(f"Install dependencies<br>{into_install:,}", BLUE, X_INSTALL, Y_PIPELINE)
    n_collection = add_node(f"pytest --collect-only<br>{into_collection:,}", BLUE, X_COLLECTION, Y_PIPELINE)
    n_final = add_node(f"Final dataset<br>{valid:,}", GREEN, X_FINAL, Y_PIPELINE)

    # -- Rejection nodes (all starting at Y_REJ, stacked vertically when sharing an x) --
    # Size filter: 1 rejection
    n_too_large = add_node(">1gb in size<br>(unknown count)", GRAY, X_SIZE + X_OFF, Y_REJ)
    # Fork filter: 1 rejection
    n_low_star_fork = add_node("Fork with <5 stars<br>(unknown count)", GRAY, X_FORK + X_OFF, Y_REJ)
    # Test file filter: 1 rejection
    n_no_test_files = add_node(
        f"No test files, or<br>has vendored<br>site-packages<br>{invalid_repo:,}", RED, X_TESTFILE + X_OFF, Y_REJ
    )
    # MinHash: 2 rejections, stacked
    n_minhash_dup = add_node(f"Duplicate<br>{minhash_duplicate:,}", RED, X_MINHASH + X_OFF, Y_REJ)
    n_minhash_err = add_node(f"Error during<br>MinHash computation<br>{minhash_error:,}", RED, X_MINHASH + X_OFF, Y_REJ + Y_REJ_STACK)
    # Install: 1 rejection
    n_install_error = add_node(f"Error during<br>installation<br>{install_error:,}", RED, X_INSTALL + X_OFF, Y_REJ)
    # Test collection: 2 rejections, stacked
    n_no_hypothesis = add_node(f"No Hypothesis tests<br>{no_hypothesis_tests:,}", RED, X_COLLECTION + X_OFF, Y_REJ)
    n_timed_out = add_node(f"Collection timed out<br>{timed_out:,}", RED, X_COLLECTION + X_OFF, Y_REJ + Y_REJ_STACK)
    # Final corpus: 1 rejection
    n_repo_404 = add_node(f"Repository later deleted<br>{repo_404:,}", ORANGE, X_FINAL + X_OFF, Y_PIPELINE + 0.15)

    # -- Links --

    # GitHub search → size filter
    add_link(n_github, n_size_filter, total, LINK_BLUE)

    # Size filter → fork filter, plus rejection
    add_link(n_size_filter, n_fork_filter, total, LINK_BLUE)
    add_link(n_size_filter, n_too_large, 1, LINK_GRAY)

    # Fork filter → test file filter, plus rejection
    add_link(n_fork_filter, n_test_file_filter, into_test_file_filter, LINK_BLUE)
    add_link(n_fork_filter, n_low_star_fork, 1, LINK_GRAY)

    # Test file filter → minhash + rejection
    add_link(n_test_file_filter, n_no_test_files, invalid_repo, LINK_RED)
    add_link(n_test_file_filter, n_minhash, into_minhash, LINK_BLUE)

    # MinHash → install + rejections
    add_link(n_minhash, n_minhash_dup, minhash_duplicate, LINK_RED)
    add_link(n_minhash, n_minhash_err, minhash_error, LINK_RED)
    add_link(n_minhash, n_install, into_install, LINK_BLUE)

    # Install → collection + rejection
    add_link(n_install, n_install_error, install_error, LINK_RED)
    add_link(n_install, n_collection, into_collection, LINK_BLUE)

    # Collection → final + rejections
    add_link(n_collection, n_no_hypothesis, no_hypothesis_tests, LINK_RED)
    add_link(n_collection, n_timed_out, timed_out, LINK_RED)
    add_link(n_collection, n_final, into_final, LINK_BLUE)

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
                    "x": node_x,
                    "y": node_y,
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
        font_size=12,
        width=1600,
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
