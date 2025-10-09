import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from github import Github

# see:
# https://docs.github.com/en/search-github/searching-on-github/searching-code
# https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-code

# 300 kb. we'll do a single open interval after this point; bank on there being
# <1000 matching files over this limit. Github claims to only index files up
# to 384kb in size, though I haven't verified this.
max_file_size = 300_000  # filesize in bytes
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


@dataclass
class RepoData:
    full_name: str
    size_bytes: int
    stargazers_count: int
    is_fork: bool


def clamp(lower, value, upper):
    return max(lower, min(value, upper))


def repos_from_term(term):
    repos = {}
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
            repo = result.repository
            # I think some of these attr accesses cost an api call, so don't
            # perform it unless we need to.
            if repo.full_name in repos:
                continue
            repo_data = RepoData(
                full_name=repo.full_name,
                size_bytes=repo.size,
                stargazers_count=repo.stargazers_count,
                is_fork=repo.fork,
            )
            repos[repo_data.full_name] = repo_data
            count_results += 1

        print(f"{count_results} results ({len(repos)} unique so far)")

        # if we hit the cap, halve the step size and retry
        if count_results >= 1000:
            step_size //= 2
            continue

        # break after the final unbounded search
        if max_size == "*":
            break

        # dynamically adjust step size for efficient searches
        if count_results > max_results / 2.5:
            step_size //= 2
        elif count_results < max_results / 3:
            step_size = int(step_size * 1.75)

        step_size = clamp(5, step_size, 8_000)
        min_size = max_size

    return repos


def repos_from_api():
    repos = {}
    for term in terms:
        repos.update(repos_from_term(term))
    return repos


def filter_repos(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT full_name, size_bytes, stargazers_count, is_fork
        FROM core_repositories
        """
    )
    repos = cursor.fetchall()
    repos_to_delete = []

    for full_name, size_bytes, stargazers_count, is_fork in repos:
        size_gb = size_bytes / 1_000_000

        if size_gb > limit_gb:
            print(f"  Rejected {full_name}: too large ({size_gb:.2f}gb)")
            repos_to_delete.append(full_name)
            continue

        if is_fork and stargazers_count < limit_forked_stars:
            print(
                f"  Rejected {full_name}: fork with {stargazers_count} < {limit_forked_stars} stars"
            )
            repos_to_delete.append(full_name)
            continue

    for full_name in repos_to_delete:
        cursor.execute(
            "DELETE FROM core_repositories WHERE full_name = ?", (full_name,)
        )
    conn.commit()

    remaining_count = cursor.execute(
        "SELECT COUNT(*) FROM core_repositories"
    ).fetchone()[0]
    conn.close()
    print(
        f"Deleted {len(repos_to_delete)} repositories, {remaining_count} remaining in {db_path}"
    )


def collect_repos(db_path):
    repos = repos_from_api()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM core_repositories")
    conn.commit()

    for repo in repos.values():
        cursor.execute(
            """
            INSERT OR REPLACE INTO core_repositories (full_name, size_bytes, stargazers_count, is_fork)
            VALUES (?, ?, ?, ?)
        """,
            (repo.full_name, repo.size_bytes, repo.stargazers_count, repo.is_fork),
        )

    conn.commit()
    count = cursor.execute("SELECT COUNT(*) FROM core_repositories").fetchone()[0]
    conn.close()

    print(f"Stored {count} repositories in {db_path}")
