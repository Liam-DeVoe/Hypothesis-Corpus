"""
Runner script for collecting GitHub repositories that use Hypothesis.
"""

from pathlib import Path

from analysis.database import Database

from .github_repos import collect_repos, filter_repos
from .minhash import minhash_repository, remove_duplicates

db_path = Path(__file__).parent.parent / "data.db"


def init_db():
    # Initialize database - this creates all core tables
    Database(db_path=str(db_path))


def process_minhashes():
    db = Database(db_path=str(db_path))
    repos = db.fetchall("SELECT id, full_name FROM core_repositories")

    print(f"Processing minhashes for {len(repos)} repositories...")
    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        print(f"[{i}/{len(repos)}] {repo_name} ... ", flush=True)
        minhash_repository(db, repo_name)


def run_collection(db_path):
    init_db()
    collect_repos(db_path)
    filter_repos(db_path)
    process_minhashes()
    remove_duplicates(db_path)
