"""
Runner script for collecting GitHub repositories that use Hypothesis.
"""

import traceback

from analysis.database import Database

from .github_repos import collect_repos, filter_repos
from .minhash import minhash_repository, remove_duplicates
from .utils import Reject


def process_minhashes(db: Database):
    repos = db.fetchall(
        """
        SELECT id, full_name FROM core_repository
        WHERE (status IS NULL OR status != 'invalid')
        AND id NOT IN (SELECT repo_id FROM core_minhashes)
    """
    )

    print(f"Processing minhashes for {len(repos)} repositories...")
    for i, repo in enumerate(repos, 1):
        repo_name = repo["full_name"]
        print(f"[{i}/{len(repos)}] {repo_name} ... ", flush=True)

        try:
            minhash_repository(db, repo_name)
        except Reject as e:
            print(f"rejected: {e}")
            db.execute(
                "UPDATE core_repository SET status = ?, status_reason = ? WHERE full_name = ?",
                ("invalid", str(e), repo_name),
            )
            db.commit()
        except Exception as e:
            # I've seen this happen for repos that were deleted
            print(f"error: {traceback.format_exception(e)}")
            db.execute(
                "UPDATE core_repository SET status = ?, status_reason = ? WHERE full_name = ?",
                ("invalid", "minhash_error", repo_name),
            )
            db.commit()


def run_collection(db_path: str):
    db = Database(db_path=db_path)
    collect_repos(db)
    filter_repos(db)
    process_minhashes(db)
    remove_duplicates(db)
