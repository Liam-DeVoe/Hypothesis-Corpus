import json
import logging
import sqlite3
from pathlib import Path

from github import Github

# see:
# https://docs.github.com/en/search-github/searching-on-github/searching-code
# https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-code

# 100 kb. we'll do a single open interval after this point; bank on there being
# <1000 matching files over this limit.
max_file_size = 100_000  # filesize in bytes
initial_step_size = 50
max_results = 1000
limit_gb = 1
limit_forked_stars = 5
terms = ["import hypothesis", "from hypothesis import", "from hypothesis."]

# silence ratelimit / backoff prints.
logging.getLogger("github").setLevel(logging.WARNING)

secrets_path = Path(__file__).parent.parent.parent / "secrets.json"
with open(secrets_path) as f:
    secrets = json.load(f)
github_token = secrets["github_token"]

g = Github(github_token, per_page=100)


def repos_from_term(term):
    repos = set()
    step_size = initial_step_size
    min_size = 0

    while True:
        max_size = min_size + step_size
        # on the last iteration, do an unbounded upwards search so we have
        # full coverage.
        if min_size >= max_file_size:
            max_size = "*"

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

        # break after the final unbounded search
        if max_size == "*":
            break

        # dynamically adjust step size for efficient searches
        if count_results > max_results / 2:
            step_size //= 2
        elif count_results < max_results / 3:
            step_size = int(step_size * 1.75)

        min_size = max_size

    return repos


def repos_from_api():
    repos = set()
    for term in terms:
        repos |= repos_from_term(term)
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
    repos = repos_from_api()
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
