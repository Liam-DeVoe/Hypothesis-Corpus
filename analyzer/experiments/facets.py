import re
import subprocess
from pathlib import Path
from typing import Any

try:
    from .experiment import Experiment
except ImportError:
    # When running as standalone module in container
    from experiment import Experiment


SUMMARY_PROMPT = """Your job is to determine what this property-based test is testing. Be clear, concise, get to the point in at most two sentences (don't say "Based on the code..."), and avoid mentioning Claude/the chatbot or using bulleted lists. For example:
<examples>
* This test verifies that a DFA with a maximum accepted string length of n returns at least 1 for count_strings on length n.
* This test verifies that reversing the bits of an integer twice (with a specified bit width n) returns the original integer, ensuring the bit reversal operation is its own inverse.
* This test verifies that the BytestringProvider correctly implements the provider contract by ensuring all drawn values satisfy their constraints and that forcing a choice to a specific value (the zeroth index value) produces the expected result when re-drawn.
</examples>
If necessary, explore the context of the test and codebase before answering. The test is located at {nodeid}. Output your answer in English inside <answer> tags."""

CATEGORY_PROMPT = """Your job is to determine a general category for a summary description of a property-based test. Be clear, concise, and get to the point in exactly one phrase. The categories should be general, without references to specifics of the codebase. Wrap each category in <category> tags. A summary might have multiple categories.

<examples>
* <category>This test checks that two functions are inverses</category>
* <category>This test compares a reference implementation to the implementation in the code</category>
* <category>This test checks that a function correctly returns False where appropriate</category>
* <category>This test checks that two functions are inverses</category><category>This test checks that an error is raised when the input is modified</category>
</examples>

Here is the summary:
<summary>
{summary}
</summary>

Now output your answer in English inside <answer> tags:"""


class FacetsExperiment(Experiment):
    name = "facets"

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS facets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                facet TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );

            CREATE INDEX IF NOT EXISTS idx_facets_node ON facets(node_id);
        """

    @staticmethod
    def _run_claude(prompt: str) -> str:
        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=60 * 10,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Claude Code failed with exit code {result.returncode}: {result.stderr}"
            )

        response_text = result.stdout
        match = re.search(r"<answer>(.*?)</answer>", response_text, re.DOTALL)

        if not match:
            raise ValueError(
                f"No <answer> tags found in response: {response_text[:200]}..."
            )

        return match.group(1).strip()

    @staticmethod
    def _run_summary(node_id: str) -> str:
        prompt = SUMMARY_PROMPT.format(nodeid=node_id)
        return FacetsExperiment._run_claude(prompt)

    @staticmethod
    def _run_category(summary: str) -> list[str]:
        """Generate category facet from a summary."""
        prompt = CATEGORY_PROMPT.format(summary=summary)
        answer = FacetsExperiment._run_claude(prompt)
        categories = re.findall(r"<category>(.*?)</category>", answer, re.DOTALL)
        return [cat.strip() for cat in categories]

    @staticmethod
    def run(file_path: Path, node_id: str) -> dict[str, Any]:
        """Run the facets experiment - generates both summary and category facets."""
        # First, generate the summary
        summary = FacetsExperiment._run_summary(node_id)
        categories = FacetsExperiment._run_category(summary)

        return {
            "summary": summary,
            "categories": categories,
        }

    @staticmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        with db.connection() as conn:
            conn.execute(
                """
                INSERT INTO facets (node_id, type, facet)
                VALUES (?, ?, ?)
                """,
                (
                    node_id,
                    "summary",
                    data["summary"],
                ),
            )

            for category in data["categories"]:
                conn.execute(
                    """
                    INSERT INTO facets (node_id, type, facet)
                    VALUES (?, ?, ?)
                    """,
                    (
                        node_id,
                        "category",
                        category,
                    ),
                )

            conn.commit()

    @staticmethod
    def delete_data(db: Any, owner: str, name: str):
        db.delete_experiment_data(owner, name, ["facets"])
