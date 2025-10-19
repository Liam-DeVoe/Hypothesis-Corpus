import logging
import os
import re
import subprocess
from typing import Any

from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

from .task import Task

logger = logging.getLogger(__name__)


CLUSTER_NAMING_PROMPT = """You are tasked with creating a clear, concise name and description for a cluster of similar items. Your goal is to identify the common theme or pattern among the items and create a descriptive label.

Here are the items in the cluster:
<items>
{items}
</items>

Analyze the items carefully and identify their common characteristics, themes, or patterns. Consider what makes these items similar to each other.

Create a short, descriptive name for the cluster (at most 10 words, likely less) that captures the essence of what these items have in common. The name should be specific and actionable.

Then write a clear, two-sentence description that explains what this cluster represents in more detail.

Present your output in the following format:
<name>[Insert your cluster name here]</name>
<description>[Insert your two-sentence description here]</description>

Be specific and accurate. Focus on what truly unifies these items."""

EMBEDDING_PROMPT = """Please provide a semantic representation of the following text. Focus on capturing the core meaning and concepts.

Text: {text}

Provide a brief semantic summary that captures the key concepts and meaning."""


class ClusterTask(Task):
    """Clusters facets using embeddings and k-means clustering (Clio-style).

    This task:
    1. Fetches all pattern and domain facets from the database
    2. Generates embeddings for each facet using Claude
    3. Runs k-means clustering to group similar facets
    4. Uses Claude to generate names and descriptions for each cluster
    5. Stores cluster assignments and metadata
    """

    name = "cluster"
    follows = ["facets"]

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS facets_cluster (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facet_type TEXT NOT NULL,  -- 'pattern' or 'domain'
                cluster_id INTEGER NOT NULL,
                cluster_name TEXT NOT NULL,
                cluster_description TEXT,
                num_items INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(facet_type, cluster_id)
            );

            CREATE TABLE IF NOT EXISTS facets_cluster_assignment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                facet_id INTEGER NOT NULL,
                facet_text TEXT NOT NULL,
                facet_type TEXT NOT NULL,  -- 'pattern' or 'domain'
                cluster_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (facet_id) REFERENCES facets(id),
                UNIQUE(facet_id)
            );

            CREATE INDEX IF NOT EXISTS idx_cluster_assignments_type ON facets_cluster_assignment(facet_type);
            CREATE INDEX IF NOT EXISTS idx_cluster_assignments_cluster ON facets_cluster_assignment(cluster_id);
        """

    # Class-level model cache to avoid reloading
    _embedding_model = None

    @staticmethod
    def _get_embedding_model() -> SentenceTransformer:
        if ClusterTask._embedding_model is None:
            logger.info("Loading sentence transformer model: all-mpnet-base-v2")
            ClusterTask._embedding_model = SentenceTransformer("all-mpnet-base-v2")
            logger.info("Model loaded successfully")
        return ClusterTask._embedding_model

    @staticmethod
    def _name_cluster(items: list[str]) -> tuple[str, str]:
        """Use Claude to generate a cluster name and description."""
        items_text = "\n".join(f"- {item}" for item in items)
        prompt = CLUSTER_NAMING_PROMPT.format(items=items_text)

        result = subprocess.run(
            ["claude", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=60 * 5,
        )

        assert result.returncode == 0

        response = result.stdout
        name = re.search(r"<name>(.*?)</name>", response, re.DOTALL).group(1).strip()
        description = (
            re.search(r"<description>(.*?)</description>", response, re.DOTALL)
            .group(1)
            .strip()
        )
        return name, description

    @staticmethod
    def _determine_optimal_k(embeddings) -> int:
        """Determine optimal number of clusters using silhouette score."""
        if len(embeddings) < 2:
            return 2

        k_min = 2
        k_max = len(embeddings) // 5

        if k_min >= k_max:
            return k_min

        best_k = k_min
        best_score = -1

        logger.info(f"Evaluating k from {k_min} to {k_max} using silhouette score...")
        for k in range(k_min, k_max + 1):
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(embeddings)
            score = silhouette_score(embeddings, labels)
            logger.info(f"  k={k}: silhouette={score:.4f}")

            if score > best_score:
                best_score = score
                best_k = k

        logger.info(f"Selected k={best_k} (silhouette={best_score:.4f})")
        return best_k

    @staticmethod
    def _cluster_facets(
        facets: list[tuple[int, str]], facet_type: str
    ) -> dict[int, dict[str, Any]]:
        """Cluster a list of facets and generate cluster metadata.

        Args:
            facets: List of (facet_id, facet_text) tuples
            facet_type: 'pattern' or 'domain'

        Returns:
            Dictionary mapping cluster_id to cluster metadata
        """
        if len(facets) < 2:
            logger.info(f"Not enough {facet_type} facets to cluster ({len(facets)})")
            return {}

        logger.info(f"Clustering {len(facets)} {facet_type} facets...")
        logger.info("Generating embeddings using all-mpnet-base-v2...")
        facet_ids = [f[0] for f in facets]
        facet_texts = [f[1] for f in facets]

        # Get embedding model and encode all texts in batch for efficiency
        model = ClusterTask._get_embedding_model()
        embeddings = model.encode(
            facet_texts, convert_to_numpy=True, show_progress_bar=True
        )
        logger.info(
            f"Generated {len(embeddings)} embeddings of dimension {embeddings.shape[1]}"
        )

        k = ClusterTask._determine_optimal_k(embeddings)
        logger.info(f"Running k-means clustering with k={k}...")
        kmeans = KMeans(n_clusters=k, random_state=42)
        cluster_labels = kmeans.fit_predict(embeddings)

        # Group facets by cluster
        clusters: dict[int, list[tuple[int, str]]] = {}
        for facet_id, facet_text, cluster_id in zip(
            facet_ids, facet_texts, cluster_labels, strict=True
        ):
            cluster_id = int(cluster_id)
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append((facet_id, facet_text))

        # Generate names and descriptions for each cluster
        logger.info("Generating cluster names and descriptions...")
        cluster_metadata = {}
        for cluster_id, cluster_facets in sorted(
            clusters.items(), key=lambda kv: kv[0]
        ):
            facet_texts_in_cluster = [f[1] for f in cluster_facets]
            name, description = ClusterTask._name_cluster(facet_texts_in_cluster)

            cluster_metadata[cluster_id] = {
                "name": name,
                "description": description,
                "facets": cluster_facets,
                "num_items": len(cluster_facets),
            }

            logger.info(f"Cluster {cluster_id}: '{name}' ({len(cluster_facets)} items)")

        return cluster_metadata

    @staticmethod
    def run(db: Any) -> dict[str, Any]:
        """Run clustering on all pattern and domain facets."""
        logger.info("Starting clustering task...")
        # gets rid of an annoying warning:
        #
        # huggingface/tokenizers: The current process just got forked, after parallelism has already been used. Disabling parallelism to avoid deadlocks...
        # To disable this warning, you can either:
        #         - Avoid using `tokenizers` before the fork if possible
        #         - Explicitly set the environment variable TOKENIZERS_PARALLELISM=(true | false)
        os.environ["TOKENIZERS_PARALLELISM"] = "false"

        # Fetch all unique patterns
        pattern_rows = db.fetchall(
            """
            SELECT DISTINCT id, facet
            FROM facets_nodes
            WHERE type = 'pattern'
            """
        )
        patterns = [(row["id"], row["facet"]) for row in pattern_rows]

        # Fetch all unique domains
        domain_rows = db.fetchall(
            """
            SELECT DISTINCT id, facet
            FROM facets_nodes
            WHERE type = 'domain'
            """
        )
        domains = [(row["id"], row["facet"]) for row in domain_rows]

        logger.info(
            f"Found {len(patterns)} unique patterns and {len(domains)} unique domains"
        )

        pattern_clusters = ClusterTask._cluster_facets(patterns, "pattern")
        domain_clusters = ClusterTask._cluster_facets(domains, "domain")

        return {
            "pattern_clusters": pattern_clusters,
            "domain_clusters": domain_clusters,
            "num_pattern_clusters": len(pattern_clusters),
            "num_domain_clusters": len(domain_clusters),
        }

    @staticmethod
    def store_to_database(db: Any, data: dict[str, Any]):
        """Store clustering results to database."""
        logger.info("Storing clustering results to database...")

        # Store pattern clusters
        for cluster_id, cluster_info in data["pattern_clusters"].items():
            # Store cluster metadata
            db.execute(
                """
                INSERT OR REPLACE INTO facets_cluster
                (facet_type, cluster_id, cluster_name, cluster_description, num_items)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "pattern",
                    cluster_id,
                    cluster_info["name"],
                    cluster_info["description"],
                    cluster_info["num_items"],
                ),
            )

            # Store cluster assignments
            for facet_id, facet_text in cluster_info["facets"]:
                db.execute(
                    """
                    INSERT OR REPLACE INTO facets_cluster_assignment
                    (facet_id, facet_text, facet_type, cluster_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (facet_id, facet_text, "pattern", cluster_id),
                )

        # Store domain clusters
        for cluster_id, cluster_info in data["domain_clusters"].items():
            # Store cluster metadata
            db.execute(
                """
                INSERT OR REPLACE INTO facets_cluster
                (facet_type, cluster_id, cluster_name, cluster_description, num_items)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "domain",
                    cluster_id,
                    cluster_info["name"],
                    cluster_info["description"],
                    cluster_info["num_items"],
                ),
            )

            # Store cluster assignments
            for facet_id, facet_text in cluster_info["facets"]:
                db.execute(
                    """
                    INSERT OR REPLACE INTO facets_cluster_assignment
                    (facet_id, facet_text, facet_type, cluster_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (facet_id, facet_text, "domain", cluster_id),
                )

        db.commit()

        logger.info(
            f"Stored {data['num_pattern_clusters']} pattern clusters and "
            f"{data['num_domain_clusters']} domain clusters"
        )

    @staticmethod
    def delete_data(db: Any):
        """Delete all clustering data."""
        logger.info("Deleting clustering data...")

        db.execute("DELETE FROM facets_cluster_assignment")
        db.execute("DELETE FROM facets_cluster")
        db.commit()

        logger.info("Clustering data deleted")
