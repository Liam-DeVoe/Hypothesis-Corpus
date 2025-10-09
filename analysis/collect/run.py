"""
Runner script for collecting GitHub repositories that use Hypothesis.
"""

import sqlite3
from pathlib import Path

from analysis.database import Database
from .github_repos import collect_repos, filter_repos
from .minhash import minhash_repository, remove_duplicates

db_path = Path(__file__).parent.parent / "data.db"


def init_db():
    # Initialize database - this creates all core tables
    Database(db_path=str(db_path))


def process_minhashes():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    repos = conn.execute("SELECT id, full_name FROM core_repositories").fetchall()

    print(f"Processing minhashes for {len(repos)} repositories...")
    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        print(f"[{i}/{len(repos)}] {repo_name} ... ", flush=True)
        minhash_repository(conn, repo_name)

    conn.close()


def run_collection(db_path):
    init_db()
    collect_repos(db_path)
    filter_repos(db_path)
    process_minhashes()
    remove_duplicates(db_path)
