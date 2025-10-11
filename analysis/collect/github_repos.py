import json
import logging
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

secrets_path = Path(__file__).parent.parent / "secrets.json"
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


def filter_repos(db):
    too_large_repos = db.fetchall(
        f"SELECT full_name, size_bytes FROM core_repository WHERE size_bytes > {limit_gb * 1_000_000}"
    )
    fork_repos = db.fetchall(
        f"SELECT full_name, stargazers_count FROM core_repository WHERE is_fork = 1 AND stargazers_count < {limit_forked_stars}"
    )

    for repo in too_large_repos:
        size_gb = repo["size_bytes"] / 1_000_000
        print(f"  Rejected {repo['full_name']}: too large ({size_gb:.2f}gb)")

    for repo in fork_repos:
        print(
            f"  Rejected {repo['full_name']}: fork with {repo['stargazers_count']} < {limit_forked_stars} stars"
        )

    db.execute(
        f"""
        UPDATE core_repository
        SET status = 'invalid', status_reason = 'too_large'
        WHERE size_bytes > {limit_gb * 1_000_000}
        """
    )

    db.execute(
        f"""
        UPDATE core_repository
        SET status = 'invalid', status_reason = 'fork'
        WHERE is_fork = 1 AND stargazers_count < {limit_forked_stars}
        """
    )
    db.commit()

    valid_count = db.fetchone(
        "SELECT COUNT(*) FROM core_repository WHERE status IS NULL OR status = 'valid'"
    )[0]
    print(
        f"Rejected {len(too_large_repos) + len(fork_repos)} repositories ({len(too_large_repos)} too large, {len(fork_repos)} forks), {valid_count} valid/unprocessed remaining"
    )


def collect_repos(db):
    repos = repos_from_api()

    db.execute("DELETE FROM core_repository")
    db.commit()

    for repo in repos.values():
        db.execute(
            """
            INSERT OR REPLACE INTO core_repository (full_name, size_bytes, stargazers_count, is_fork)
            VALUES (?, ?, ?, ?)
        """,
            (repo.full_name, repo.size_bytes, repo.stargazers_count, repo.is_fork),
        )

    db.commit()
    count = db.fetchone("SELECT COUNT(*) FROM core_repository")[0]
    print(f"Stored {count} repositories")
