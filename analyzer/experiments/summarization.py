import re
import subprocess
from pathlib import Path
from typing import Any

try:
    from .experiment import Experiment
except ImportError:
    Experiment = object


PROMPT = """Your job is to determine what this property-based test is testing. Be clear and concise and get to the point in at most two sentences (don't say "Based on the code..."), and avoid mentioning Claude/the chatbot or using bulleted lists. For example:
<examples>
* This test verifies that a DFA with a maximum accepted string length of n returns at least 1 for count_strings on length n.
* This test verifies that reversing the bits of an integer twice (with a specified bit width n) returns the original integer, ensuring the bit reversal operation is its own inverse.
* This test verifies that the BytestringProvider correctly implements the provider contract by ensuring all drawn values satisfy their constraints and that forcing a choice to a specific value (the zeroth index value) produces the expected result when re-drawn.
</examples>
If necessary, explore the context of the test and codebase before answering. The test is located at {nodeid}. Output your answer in English inside <answer> tags."""


class SummarizationExperiment(Experiment):
    name = "summarization"

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS summarization (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                summary TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_summarization_node ON summarization(node_id);
        """

    @staticmethod
    def run(file_path: Path, node_id: str) -> dict[str, Any]:
        """Run the summarization experiment."""
        prompt = PROMPT.format(nodeid=node_id)
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=60 * 10,
        )

        if result.returncode != 0:
            return {
                "error": f"Claude Code failed with exit code {result.returncode}: {result.stderr}",
                "summary": None,
            }

        response_text = result.stdout
        match = re.search(r"<answer>(.*?)</answer>", response_text, re.DOTALL)

        if not match:
            return {
                "error": f"No <answer> tags found in response: {response_text[:200]}...",
                "summary": None,
            }

        return {
            "summary": match.group(1).strip(),
            "error": None,
        }

    @staticmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        """Store summarization results to database."""
        with db.connection() as conn:
            conn.execute(
                """
                INSERT INTO summarization (node_id, summary)
                VALUES (?, ?)
                """,
                (
                    node_id,
                    data.get("summary"),
                ),
            )
            conn.commit()

    @staticmethod
    def delete_data(db: Any, owner: str, name: str):
        """Delete summarization data."""
        with db.connection() as conn:
            result = conn.execute(
                "SELECT id FROM repositories WHERE owner = ? AND name = ?",
                (owner, name),
            ).fetchone()

            if not result:
                return

            repo_id = result["id"]

            node_ids = conn.execute(
                "SELECT id FROM nodes WHERE repo_id = ?", (repo_id,)
            ).fetchall()
            node_id_list = [row["id"] for row in node_ids]

            if node_id_list:
                placeholders = ",".join("?" * len(node_id_list))
                conn.execute(
                    f"DELETE FROM summarization WHERE node_id IN ({placeholders})",
                    node_id_list,
                )

            conn.commit()
