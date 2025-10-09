"""
Runner script for collecting GitHub repositories that use Hypothesis.
"""

import sqlite3
from pathlib import Path

from github_repos import collect_repos, filter_repos
from minhash import minhash_repository, remove_duplicates

db_path = Path(__file__).parent / "data.db"


def init_db():
    """Initialize the database schema."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repositories (
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
        CREATE TABLE IF NOT EXISTS minhash_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id INTEGER NOT NULL,
            minhash_data BLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (repo_id) REFERENCES repositories(id)
        );

        CREATE INDEX IF NOT EXISTS idx_minhash_repo ON minhash_files(repo_id);
        """
    )
    conn.commit()
    conn.close()


def process_minhashes():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    repos = conn.execute("SELECT id, full_name FROM repositories").fetchall()

    print(f"\nProcessing minhashes for {len(repos)} repositories...")

    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        print(f"[{i}/{len(repos)}] {repo_name} ... ", end="", flush=True)

        try:
            minhash_repository(conn, repo_name)
            print("done")
        except Exception as e:
            print(f"failed: {e}")

    conn.close()


if __name__ == "__main__":
    init_db()
    collect_repos(db_path)
    filter_repos(db_path)
    process_minhashes()
    remove_duplicates(db_path)
