import sqlite3
from collections import Counter

import plotly.graph_objects as go

# Connect to database
db_path = "/Users/tybug/Desktop/data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all repositories with their status and status_reason
cursor.execute(
    """
    SELECT status, status_reason, collection_returncode
    FROM core_repository
"""
)
repos = cursor.fetchall()

total_repos = len(repos)

# Categorize repositories by their journey through the pipeline
# Stage 1: GitHub Search -> Collected
# Stage 2: Installation attempt (has collection_returncode OR install_error)
# Stage 3: Final status (valid/invalid with reasons)

# install_error without returncode = exception during installation (attempted but crashed)
has_returncode = sum(1 for _, _, rc in repos if rc is not None)
install_error_crashed = sum(
    1 for _, r, rc in repos if rc is None and "install_error" in (r or "")
)
attempted_installation = has_returncode + install_error_crashed
no_returncode = total_repos - attempted_installation

# For repos with returncode, categorize by status
valid_repos = sum(1 for s, _, rc in repos if s == "valid" and rc is not None)
invalid_repos = sum(1 for s, _, rc in repos if s == "invalid" and rc is not None)

# Get detailed breakdown of invalid reasons
invalid_reasons = Counter()
for status, reason, rc in repos:
    if status == "invalid" and rc is not None:
        if "no_hypothesis_tests" in (reason or ""):
            invalid_reasons["Pytest collected 0 Hypothesis tests"] += 1
        elif reason == "invalid_repo":
            invalid_reasons["Invalid repository"] += 1
        elif "install_error" in (reason or ""):
            invalid_reasons["Install error"] += 1
        elif "timed_out" in (reason or ""):
            invalid_reasons["Install timeout"] += 1
        elif "minhash_duplicate" in (reason or ""):
            invalid_reasons["Duplicate (MinHash)"] += 1
        elif "minhash_error" in (reason or ""):
            invalid_reasons["MinHash error"] += 1
        else:
            invalid_reasons["Other"] += 1

# For repos without returncode (excluding install_error which crashed during attempt)
never_installed_reasons = Counter()
for status, reason, rc in repos:
    if rc is None and "install_error" not in (reason or ""):
        if reason == "invalid_repo":
            never_installed_reasons[
                "Rejected after clone (no test_*.py files, etc)"
            ] += 1
        elif "minhash_duplicate" in (reason or ""):
            never_installed_reasons["Duplicate (as determined by MinHash)"] += 1
        elif "minhash_error" in (reason or ""):
            never_installed_reasons["Error during minhash processing"] += 1
        else:
            never_installed_reasons["Other/unprocessed"] += 1

# Build Sankey nodes and links
nodes = []
links = {"source": [], "target": [], "value": [], "label": [], "color": []}


def add_node(label):
    nodes.append(label)
    return len(nodes) - 1


def add_link(source_idx, target_idx, value, label="", color=None):
    links["source"].append(source_idx)
    links["target"].append(target_idx)
    links["value"].append(value)
    links["label"].append(label)
    links["color"].append(color or "rgba(200, 200, 200, 0.4)")


# Node colors
node_colors = []

# Stage 0: GitHub search results
start_idx = add_node(f"github search<br>{total_repos:,} repos")
node_colors.append("#636EFA")  # Blue - start

# Stage 1: Fork based on whether installation was attempted
attempted_idx = add_node(f"Installation<br>Attempted<br>({attempted_installation:,})")
node_colors.append("#FFA15A")  # Orange - processing

not_attempted_idx = add_node(f"Installation not attempted<br>({no_returncode:,})")
node_colors.append("#EF553B")  # Red - rejected early

add_link(
    start_idx, attempted_idx, attempted_installation, color="rgba(255, 161, 90, 0.4)"
)
add_link(start_idx, not_attempted_idx, no_returncode, color="rgba(239, 85, 59, 0.4)")

# Stage 2: Reasons for not attempting
for reason, count in never_installed_reasons.most_common():
    idx = add_node(f"{reason}<br>({count:,})")
    node_colors.append("#EF553B")  # Red
    add_link(not_attempted_idx, idx, count, color="rgba(239, 85, 59, 0.3)")

# Stage 3: Installation outcomes
valid_idx = add_node(f"Valid<br>{valid_repos:,} repos")
node_colors.append("#00CC96")  # Green - success

invalid_idx = add_node(f"Invalid<br>({invalid_repos:,})")
node_colors.append("#EF553B")  # Red - failed

crashed_idx = add_node(f"Error during installation<br>({install_error_crashed:,})")
node_colors.append("#EF553B")

add_link(attempted_idx, valid_idx, valid_repos, color="rgba(0, 204, 150, 0.4)")
add_link(attempted_idx, invalid_idx, invalid_repos, color="rgba(239, 85, 59, 0.4)")
add_link(
    attempted_idx, crashed_idx, install_error_crashed, color="rgba(239, 85, 59, 0.4)"
)

# Stage 4: Invalid reasons breakdown
for reason, count in invalid_reasons.most_common():
    idx = add_node(f"{reason}<br>({count:,})")
    node_colors.append("#EF553B")  # Red
    add_link(invalid_idx, idx, count, color="rgba(239, 85, 59, 0.3)")

# Create Sankey diagram
fig = go.Figure(
    data=[
        go.Sankey(
            node=dict(
                pad=20,
                thickness=25,
                line=dict(color="white", width=2),
                label=nodes,
                color=node_colors,
            ),
            link=dict(
                source=links["source"],
                target=links["target"],
                value=links["value"],
                label=links["label"],
                color=links["color"],
            ),
        )
    ]
)


# Save as HTML
output_path = "/Users/tybug/Desktop/Liam/coding/research/empirical-pbt-experimental/sankey_diagram.html"
fig.write_html(output_path)
print(f"Sankey diagram saved to: {output_path}")

for reason, count in invalid_reasons.most_common(5):
    print(f"  {reason}: {count:,} ({100*count/invalid_repos:.1f}%)")

conn.close()
