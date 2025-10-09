"""
Runner script for collecting GitHub repositories that use Hypothesis.
"""

import sqlite3
from pathlib import Path

from github_repos import collect_repos, filter_repos
from minhash import minhash_repository, remove_duplicates

db_path = Path(__file__).parent.parent / "data.db"


def init_db():
    """Initialize the database schema."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS core_repositories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT UNIQUE NOT NULL,
            size_bytes INTEGER NOT NULL,
            stargazers_count INTEGER NOT NULL,
            is_fork BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS core_minhashes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            minhash_data BLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (repo_id) REFERENCES core_repositories(id)
        );

        CREATE INDEX IF NOT EXISTS idx_core_minhashes_repo ON core_minhashes(repo_id);
        """
    )
    conn.commit()
    conn.close()


def process_minhashes():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    repos = conn.execute("SELECT id, full_name FROM core_repositories").fetchall()

    print(f"Processing minhashes for {len(repos)} repositories...")
    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        print(f"[{i}/{len(repos)}] {repo_name} ... ", end="", flush=True)
        minhash_repository(conn, repo_name)

    conn.close()


if __name__ == "__main__":
    init_db()
    collect_repos(db_path)
    filter_repos(db_path)
    process_minhashes()
    remove_duplicates(db_path)
