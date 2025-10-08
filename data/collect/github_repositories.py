import json
import sqlite3
from pathlib import Path

from github import Github

# 100 kb. we'll do a single open interval after this point; bank on there being
# <1000 matching files over this limit.
max_file_size = 100  # filesize in bytes
step_size = 100
limit_gb = 1
limit_forked_stars = 5
terms = ["import hypothesis", "from hypothesis import", "from hypothesis."]


secrets_path = Path(__file__).parent.parent.parent / "secrets.json"
with open(secrets_path) as f:
    secrets = json.load(f)
github_token = secrets["github_token"]

# https://docs.github.com/en/search-github/searching-on-github/searching-code
g = Github(github_token, per_page=100)


def all_repos():
    repos = set()
    for i in range(max_file_size // step_size):
        min_size = i * step_size
        max_size = (i + 1) * step_size
        # on the last iteration, do an unbounded upwards search so we have
        # full coverage.
        if min_size == max_size:
            max_size = "*"
        for term in terms:
            q = f'size:{min_size}..{max_size} "{term}"'
            print(f"{q} ... ", end="", flush=True)

            results = g.search_code(q)
            count_results = 0
            for result in results:
                repos.add(result.repository)
                count_results += 1

            print(f"{count_results} results ({len(repos)} unique so far)")
            # make sure we're not missing any results by hitting the cap.
            assert count_results < 1000
    return repos


def filter_repos(repos):
    filtered_repos = []

    for repo in repos:
        size_gb = repo.size / 1_000_000
        if size_gb > limit_gb:
            print(f"  Rejected {repo.full_name}: too large ({size_gb:.2f}gb)")
            continue

        if repo.fork and repo.stargazers_count < limit_forked_stars:
            print(
                f"  Rejected {repo.full_name}: fork with {repo.stargazers_count} < {limit_forked_stars} stars"
            )
            continue

        filtered_repos.append(repo)

    return filtered_repos


def collect_github_repositories(db_path):
    repos = all_repos()
    repos = filter_repos(repos)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM repositories")
    conn.commit()

    for repo in repos:
        cursor.execute(
            """
            INSERT OR REPLACE INTO repositories (full_name, size_bytes, stargazers_count, is_fork)
            VALUES (?, ?, ?, ?)
        """,
            (repo.full_name, repo.size, repo.stargazers_count, repo.fork),
        )

    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM repositories").fetchone()[0]
    conn.close()

    print(f"\nStored {count} repositories in {db_path}")
