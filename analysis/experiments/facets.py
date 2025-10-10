import re
from pathlib import Path
from typing import Any

try:
    from .experiment import Experiment
    from .utils import subprocess_run
except ImportError:
    # When running as standalone module in container
    from experiment import Experiment
    from utils import subprocess_run


SUMMARY_PROMPT = """Your job is to summarize what this property-based test is testing. Describe both the testing pattern/relationship being verified and the domain/technology being tested. Be clear, concise, and get to the point in at most two sentences. Focus on what is being tested and how, not on implementation details.

<examples>
* This test verifies that a DFA correctly counts accepted strings at the maximum length boundary by checking that count_strings returns at least 1 for strings of length n when the maximum is n.
* This test verifies that bit reversal operations are inverses by checking that reversing the bits of an integer twice (with a specified bit width) returns the original integer.
* This test verifies that BytestringProvider implements the provider contract correctly by checking that drawn values satisfy constraints and that forcing a choice produces the expected result when re-drawn.
* This test verifies that JSON serialization and deserialization are inverses by checking that parsing the serialized output of an object returns an equivalent object.
</examples>

If necessary, explore the context of the test and codebase before answering. The test is located at {nodeid}. Output your answer in English inside <answer> tags."""

PATTERN_PROMPT = """Your job is to determine the kind of property being tested based on a summary description of a property-based test. Focus on the general testing pattern or relationship being verified, not on the specific domain or technologies involved. Be clear, concise, and get to the point in exactly one phrase. Avoid referencing specifics of the codebase or domain. Avoid overly general descriptions like "tests that behavior is correct". Wrap each pattern in <property_pattern> tags. A summary might have multiple patterns.

<examples>
* <property_pattern>inverse relationship between two functions</property_pattern>
* <property_pattern>equivalence with reference implementation</property_pattern>
* <property_pattern>correct boolean return value for specific conditions</property_pattern>
* <property_pattern>inverse relationship between two functions</property_pattern><property_pattern>error raised on invalid input</property_pattern>
* <property_pattern>idempotence of repeated operations</property_pattern>
* <property_pattern>invariant preservation across transformations</property_pattern>
</examples>

Here is the summary:
<summary>
{summary}
</summary>

Now output your answer in English inside <answer> tags:"""


DOMAIN_PROMPT = """Your job is to determine the technical programming domain being tested based on a summary description of a property-based test. Focus on the specific technologies, data structures, algorithms, or system components being tested, not on the testing strategy itself. Be clear, concise, and get to the point in exactly one phrase. Wrap each domain in <domain> tags. A summary might involve multiple domains.

<examples>
* <domain>file format serialization</domain>
* <domain>cryptographic operations</domain>
* <domain>stateful REST API interactions</domain>
* <domain>file system operations</domain>
* <domain>datetime arithmetic</domain><domain>timezone handling</domain>
* <domain>JSON schema validation</domain>
* <domain>concurrent data structures</domain>
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
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE INDEX IF NOT EXISTS idx_facets_node ON facets(node_id);
        """

    @staticmethod
    def _run_claude(prompt: str) -> str:
        result = subprocess_run(
            ["claude", "-p", prompt],
            timeout=60 * 10,
        )

        assert result.returncode == 0
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
    def _run_pattern(summary: str) -> list[str]:
        """Generate pattern facets from a summary."""
        prompt = PATTERN_PROMPT.format(summary=summary)
        answer = FacetsExperiment._run_claude(prompt)
        patterns = re.findall(
            r"<property_pattern>(.*?)</property_pattern>", answer, re.DOTALL
        )
        return [pattern.strip() for pattern in patterns]

    @staticmethod
    def _run_domain(summary: str) -> list[str]:
        """Generate domain facets from a summary."""
        prompt = DOMAIN_PROMPT.format(summary=summary)
        answer = FacetsExperiment._run_claude(prompt)
        domains = re.findall(r"<domain>(.*?)</domain>", answer, re.DOTALL)
        return [domain.strip() for domain in domains]

    @staticmethod
    def run(file_path: Path, node_id: str, debug) -> dict[str, Any]:
        """Run the facets experiment - generates summary, pattern, and domain facets."""
        # First, generate the summary
        summary = FacetsExperiment._run_summary(node_id)

        # Then, generate patterns and domains from the summary
        patterns = FacetsExperiment._run_pattern(summary)
        domains = FacetsExperiment._run_domain(summary)

        return {
            "summary": summary,
            "patterns": patterns,
            "domains": domains,
        }

    @staticmethod
    def store_to_database(db: Any, repo_id: int, node_id: int, data: dict[str, Any]):
        db.execute(
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

        for pattern in data["patterns"]:
            db.execute(
                """
                INSERT INTO facets (node_id, type, facet)
                VALUES (?, ?, ?)
                """,
                (
                    node_id,
                    "pattern",
                    pattern,
                ),
            )

        for domain in data["domains"]:
            db.execute(
                """
                INSERT INTO facets (node_id, type, facet)
                VALUES (?, ?, ?)
                """,
                (
                    node_id,
                    "domain",
                    domain,
                ),
            )

        db.commit()

    @staticmethod
    def delete_data(db: Any, repo_name: str):
        db.delete_experiment_data(repo_name, ["facets"])
