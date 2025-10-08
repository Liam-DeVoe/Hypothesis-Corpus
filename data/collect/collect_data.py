from github import Github
from pathlib import Path
import json

# TODO NEXT:
# * deduplicate repos by minhash
# * split analysis into:
#   * all repos
#   * runnable repos

secrets_path = Path(__file__).parent.parent.parent / "secrets.json"
with open(secrets_path) as f:
    secrets = json.load(f)
github_token = secrets["github_token"]

# 100 kb. we'll do a single open interval after this point; bank on there being
# <1000 matching files over this limit.
max_file_size = 100_000  # filesize in bytes
step_size = 100
limit_gb = 1
limit_forked_stars = 5
terms = ["import hypothesis", "from hypothesis import", "from hypothesis."]
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
            print(f"  Rejected {repo.full_name}: fork with {repo.stargazers_count} < {limit_forked_stars} stars")
            continue

        filtered_repos.append(repo)

    return filtered_repos

repos = all_repos()
repos = filter_repos(repos)
repo_data = [repo.full_name for repo in repos]
repo_data = json.dumps(repo_data)
(Path(__file__).parent / "data.json").write_text(repo_data)
