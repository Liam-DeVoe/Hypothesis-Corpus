import re
from typing import Any

try:
    from .experiment import Experiment
    from .utils import subprocess_run
except ImportError:
    # When running as standalone module in container
    from experiment import Experiment
    from utils import subprocess_run


REPOSITORY_SUMMARY_PROMPT = """Your job is to summarize what this GitHub repository does. Describe the repository's purpose and primary functionality. Be clear, concise, and get to the point in at most two sentences.

<examples>
* <summary>This repository implements a finite automata library for Python, providing classes and utilities for creating, manipulating, and querying deterministic and non-deterministic finite automata.</summary>
* <summary>This repository provides cryptographic primitives and utilities for Python, including implementations of various encryption algorithms, hash functions, and key generation utilities.</summary>
* <summary>This repository is a data serialization library supporting multiple formats (JSON, YAML, XML) with schema validation and type conversion capabilities.</summary>
* <summary>This repository provides date and time utilities for Python, offering timezone-aware datetime manipulation, parsing, formatting, and arithmetic operations.</summary>
</examples>

If necessary, explore the repository codebase before answering. The repository's name is {repo_name}. Output your answer in English inside <summary> tags."""

NODE_SUMMARY_PROMPT = """Your job is to summarize what this property-based test is testing. Describe both the testing pattern/relationship being verified and the domain/technology being tested. Be clear, concise, and get to the point in at most two sentences. Focus on what is being tested and how, not on implementation details.

<examples>
* <summary>This test verifies that a DFA correctly counts accepted strings at the maximum length boundary by checking that count_strings returns at least 1 for strings of length n when the maximum is n.</summary>
* <summary>This test verifies that bit reversal operations are inverses by checking that reversing the bits of an integer twice (with a specified bit width) returns the original integer.</summary>
* <summary>This test verifies that BytestringProvider implements the provider contract correctly by checking that drawn values satisfy constraints and that forcing a choice produces the expected result when re-drawn.</summary>
* <summary>This test verifies that JSON serialization and deserialization are inverses by checking that parsing the serialized output of an object returns an equivalent object.</summary>
</examples>

If necessary, explore the context of the test and codebase before answering. The test is located at {nodeid}. Output your answer in English inside <summary> tags."""

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

Output your answer(s) in English inside <property_pattern> tags."""


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

Output your answer(s) in English inside <domain> tags."""

CLAUDE_MODEL = "claude-haiku-4-5-20251001"


class FacetsExperiment(Experiment):
    name = "facets"
    only_canonical_nodes = True

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS facets_repository (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                facet TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (repo_id) REFERENCES core_repository(id)
            );

            CREATE TABLE IF NOT EXISTS facets_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                facet TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );

            CREATE INDEX IF NOT EXISTS idx_facets_repository_repo ON facets_repository(repo_id);
            CREATE INDEX IF NOT EXISTS idx_facets_repository_type ON facets_repository(type);

            CREATE INDEX IF NOT EXISTS idx_facets_nodes_node ON facets_nodes(node_id);
            CREATE INDEX IF NOT EXISTS idx_facets_nodes_type ON facets_nodes(type);
        """

    @staticmethod
    def _run_claude(prompt: str) -> str:
        result = subprocess_run(
            ["claude", "--model", CLAUDE_MODEL, "-p", prompt],
            timeout=60 * 10,
            cwd="/app/repo",
        )

        assert result.returncode == 0, (result.stdout, result.stderr)
        return result.stdout

    @staticmethod
    def _run_summary(node_id: str) -> str:
        prompt = NODE_SUMMARY_PROMPT.format(nodeid=node_id)
        response = FacetsExperiment._run_claude(prompt)
        match = re.search(r"<summary>(.*?)</summary>", response, re.DOTALL)
        if not match:
            raise ValueError(f"No <summary> tags found in response: {response}...")

        return match.group(1).strip()

    @staticmethod
    def _run_pattern(summary: str) -> list[str]:
        """Generate pattern facets from a summary."""
        prompt = PATTERN_PROMPT.format(summary=summary)
        response = FacetsExperiment._run_claude(prompt)
        patterns = re.findall(
            r"<property_pattern>(.*?)</property_pattern>", response, re.DOTALL
        )
        return [pattern.strip() for pattern in patterns]

    @staticmethod
    def _run_domain(summary: str) -> list[str]:
        """Generate domain facets from a summary."""
        prompt = DOMAIN_PROMPT.format(summary=summary)
        response = FacetsExperiment._run_claude(prompt)
        domains = re.findall(r"<domain>(.*?)</domain>", response, re.DOTALL)
        return [domain.strip() for domain in domains]

    @staticmethod
    def run(node_id: str, debug) -> dict[str, Any]:
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
            INSERT INTO facets_nodes (node_id, type, facet)
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
                INSERT INTO facets_nodes (node_id, type, facet)
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
                INSERT INTO facets_nodes (node_id, type, facet)
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
    def run_repository(repo_name: str, node_ids: list[str]) -> dict[str, Any]:
        prompt = REPOSITORY_SUMMARY_PROMPT.format(repo_name=repo_name)
        response = FacetsExperiment._run_claude(prompt)
        match = re.search(r"<summary>(.*?)</summary>", response, re.DOTALL)
        if not match:
            raise ValueError(f"No <summary> tags found in response: {response}...")
        summary = match.group(1).strip()
        return {"summary": summary}

    @staticmethod
    def store_repository_to_database(db: Any, repo_id: int, data: dict[str, Any]):
        """Store repository-level results to the database."""
        db.execute(
            """
            INSERT INTO facets_repository (repo_id, type, facet)
            VALUES (?, ?, ?)
            """,
            (
                repo_id,
                "summary",
                data["summary"],
            ),
        )
        db.commit()

    @staticmethod
    def delete_data(db: Any, repo_id: int):
        db.execute("DELETE FROM facets_repository WHERE repo_id = ?", (repo_id,))

        node_ids = db.fetchall("SELECT id FROM core_node WHERE repo_id = ?", (repo_id,))
        node_id_list = [row["id"] for row in node_ids]
        if node_id_list:
            placeholders = ",".join("?" * len(node_id_list))
            db.execute(
                f"DELETE FROM facets_nodes WHERE node_id IN ({placeholders})",
                node_id_list,
            )

        db.commit()
