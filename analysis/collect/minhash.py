# repository deduplication via minhash.
#
# We use line-level shingles. word-level is too coarse because all repos share common
# keywords like "def", "class", etc. We normalize by stripping whitespace.
# (A "shingle" is an overlapping sliding window, though in the case of 1 line per
# shingle there is no overlap.)
#
# Each .py file in a repo gets a minhash. We compute the number of duplicate files
# between two repos by looking at bruteforce pairwise-combinations of the minhash
# jaccard similarity, at a given threshold level.

import pickle
import subprocess
import tempfile
from pathlib import Path

from datasketch import MinHash

num_perm = 128
# skip hashing files with fewer lines than this
min_file_lines = 25
# if both:
#
# * 80% of files from repo1 are in repo2
# * 80% of files from repo2 are in repo1
#
# then count it as a dupliate. We keep the one with a higher star count.
duplicate_overlap_threshold = 0.8


def compute_minhashes(repo_name: str) -> list[MinHash]:
    assert "/" in repo_name

    minhashes = []
    with tempfile.TemporaryDirectory() as tmpdir:
        clone_path = Path(tmpdir) / "repo"
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--quiet",
                f"https://github.com/{repo_name}.git",
                str(clone_path),
            ],
            check=True,
        )

        for file_path in clone_path.rglob("*.py"):
            if not file_path.is_file():
                continue
            if ".git" in file_path.parts:
                continue

            try:
                content = file_path.read_text(encoding="utf-8", errors="ignore")
                lines = content.splitlines()
                if len(lines) < min_file_lines:
                    continue

                m = MinHash(num_perm=num_perm)
                for line in lines:
                    normalized = line.strip()
                    # normalize whitespace. skip empty lines.
                    if normalized:
                        m.update(normalized.encode("utf-8"))

                minhashes.append(m)

            except Exception:
                continue

    return minhashes


def load_minhashes(db, repo_name: str) -> list[MinHash] | None:
    result = db.fetchone(
        "SELECT id FROM core_repository WHERE full_name = ?",
        (repo_name,),
    )
    assert result

    repo_id = result["id"]
    rows = db.fetchall(
        "SELECT minhash_data FROM core_minhashes WHERE repo_id = ?",
        (repo_id,),
    )
    return [pickle.loads(row["minhash_data"]) for row in rows]


def minhash_repository(db, repo_name: str):
    minhashes = compute_minhashes(repo_name)

    result = db.fetchone(
        "SELECT id FROM core_repository WHERE full_name = ?",
        (repo_name,),
    )
    assert result, f"Repository {repo_name} not found"
    repo_id = result["id"]

    db.execute("DELETE FROM core_minhashes WHERE repo_id = ?", (repo_id,))
    for minhash in minhashes:
        minhash_blob = pickle.dumps(minhash)
        db.execute(
            "INSERT INTO core_minhashes (repo_id, minhash_data) VALUES (?, ?)",
            (repo_id, minhash_blob),
        )

    db.commit()


def count_duplicates(
    minhashes1: list[MinHash],
    minhashes2: list[MinHash],
    *,
    threshold: float = 0.95,
) -> int:
    """
    Returns the number of files in `minhashes1` which are also present in
    `minhashes2`, up to `threshold` similarity.
    """

    count = 0
    for minhash1 in minhashes1:
        for minhash2 in minhashes2:
            similarity = minhash1.jaccard(minhash2)
            if similarity >= threshold:
                count += 1
                # maximum one match per file
                break

    return count


def remove_duplicates(db_path):
    """Identify and remove duplicate repositories based on minhash similarity."""
    from analysis.database import Database

    db = Database(db_path=db_path)
    repos = db.fetchall("SELECT id, full_name, stargazers_count FROM core_repository")
    print(f"Removing duplicates among {len(repos)} repositories...")

    to_remove = set()

    for i, repo1 in enumerate(repos):
        if repo1["full_name"] in to_remove:
            continue

        repo1_name = repo1["full_name"]
        mh1 = load_minhashes(db, repo1_name)
        if not mh1:
            continue

        for repo2 in repos[i + 1 :]:
            if repo2["full_name"] in to_remove:
                continue

            repo2_name = repo2["full_name"]
            mh2 = load_minhashes(db, repo2_name)
            if not mh2:
                continue

            overlap_1 = count_duplicates(mh1, mh2) / len(mh1)
            overlap_2 = count_duplicates(mh2, mh1) / len(mh2)

            if (
                overlap_1 >= duplicate_overlap_threshold
                and overlap_2 >= duplicate_overlap_threshold
            ):
                print(
                    f"  Duplicate: {repo1_name} ↔ {repo2_name} ({overlap_1:.1%}/{overlap_2:.1%})"
                )
                # Keep the one with more stars, remove the other
                stars1 = repo1["stargazers_count"]
                stars2 = repo2["stargazers_count"]
                if stars1 >= stars2:
                    to_remove.add(repo2_name)
                else:
                    to_remove.add(repo1_name)
                    break  # This repo is being removed, move to next

    print(f"\nRemoving {len(to_remove)} duplicate repositories...")
    for repo_name in to_remove:
        db.execute("DELETE FROM core_repository WHERE full_name = ?", (repo_name,))
    db.commit()
    print(f"Kept {len(repos) - len(to_remove)} unique repositories")
