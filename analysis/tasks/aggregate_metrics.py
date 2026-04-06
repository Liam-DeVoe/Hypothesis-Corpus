"""Pre-compute per-node aggregate metrics from runtime_test_case for fast
dashboard queries."""

import json
import logging
from typing import Any

import pandas as pd

from .task import Task

logger = logging.getLogger(__name__)


class AggregateMetricsTask(Task):
    name = "aggregate_metrics"
    tables = ["node_aggregate_metrics"]
    follows = ["runtime"]

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS node_aggregate_metrics (
                node_id INTEGER PRIMARY KEY,
                median_execution_time REAL,
                median_generation_percent REAL,
                generation_percent REAL,
                execution_time_cv REAL,
                percent_overrun REAL,
                percent_invalid REAL,
                median_feature_count REAL,
                min_choices_size INTEGER,
                median_choices_size REAL,
                max_choices_size INTEGER,
                generation_curve TEXT,
                FOREIGN KEY (node_id) REFERENCES core_node(id)
            );
        """

    @staticmethod
    def run(db: Any) -> dict[str, Any]:
        # Query 1: SQL-computable aggregates (single GROUP BY scan)
        logger.info("Computing SQL aggregates...")
        sql_agg = pd.read_sql_query(
            """
            SELECT
                node_id,
                SUM(CASE WHEN data_status = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as percent_overrun,
                SUM(CASE WHEN data_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as percent_invalid,
                MIN(choices_size) as min_choices_size,
                MAX(choices_size) as max_choices_size,
                AVG(execution_time) as mean_execution_time,
                AVG(execution_time * execution_time) - AVG(execution_time) * AVG(execution_time) as execution_time_variance,
                COUNT(execution_time) as execution_time_count
            FROM (
                SELECT
                    node_id,
                    data_status,
                    choices_size,
                    json_extract(timing, '$."execute:test"') as execution_time
                FROM runtime_test_case
            )
            GROUP BY node_id
            """,
            db._conn,
        )
        logger.info(f"  {len(sql_agg)} nodes from SQL aggregates")

        # Compute CV from variance
        sql_agg["execution_time_cv"] = None
        mask = (sql_agg["execution_time_count"] >= 10) & (
            sql_agg["mean_execution_time"] > 0
        )
        sql_agg.loc[mask, "execution_time_cv"] = (
            sql_agg.loc[mask, "execution_time_variance"].clip(lower=0) ** 0.5
            / sql_agg.loc[mask, "mean_execution_time"]
        )

        # Query 2: Medians for choices_size and execution_time
        # These are numeric columns, so the transfer is manageable (~15M rows × 3 cols)
        logger.info("Computing medians for choices_size and execution_time...")
        medians_data = pd.read_sql_query(
            """
            SELECT
                node_id,
                choices_size,
                json_extract(timing, '$."execute:test"') as execution_time
            FROM runtime_test_case
            """,
            db._conn,
        )
        logger.info(f"  loaded {len(medians_data)} rows")

        median_choices = medians_data.groupby("node_id")["choices_size"].median()
        median_execution = medians_data.groupby("node_id")["execution_time"].median()

        # Query 3: Median generation % and generation curve
        # (needs full timing JSON — one-time cost)
        logger.info("Computing median generation % and generation curves...")
        timing_data = pd.read_sql_query(
            """
            SELECT node_id, test_case_number, timing
            FROM runtime_test_case
            """,
            db._conn,
        )
        logger.info(f"  loaded {len(timing_data)} rows")

        gen_rows = []
        for _, row in timing_data.iterrows():
            timing = json.loads(row["timing"])
            execution_time = sum(
                v for k, v in timing.items() if k.startswith("execute:")
            )
            gen_time = sum(v for k, v in timing.items() if k.startswith("generate:"))
            total = execution_time + gen_time
            if total > 0:
                gen_rows.append(
                    {
                        "node_id": row["node_id"],
                        "test_case_number": row["test_case_number"],
                        "gen_percent": gen_time / total * 100,
                        "gen_time": gen_time,
                        "total_time": total,
                    }
                )

        generation_curves = pd.Series(dtype=object)
        if gen_rows:
            gen_df = pd.DataFrame(gen_rows)
            median_gen_percent = gen_df.groupby("node_id")["gen_percent"].median()
            gen_sums = gen_df.groupby("node_id").agg(
                total_gen=("gen_time", "sum"), total_time=("total_time", "sum")
            )
            gen_percent_overall = gen_sums["total_gen"] / gen_sums["total_time"] * 100

            # Per-node generation curves: bin testcases by % through run
            max_tc = gen_df.groupby("node_id")["test_case_number"].max()
            gen_df = gen_df.merge(max_tc.rename("max_tc"), on="node_id")
            gen_df = gen_df[gen_df["max_tc"] > 0]
            gen_df["bin"] = (
                (gen_df["test_case_number"] / gen_df["max_tc"] * 100)
                .round()
                .clip(upper=100)
                .astype(int)
            )
            node_bin_avg = gen_df.groupby(["node_id", "bin"])["gen_percent"].mean()
            generation_curves = node_bin_avg.groupby(level="node_id").apply(
                lambda g: json.dumps(
                    {int(b): round(v, 4) for b, v in g.droplevel(0).items()}
                )
            )
            logger.info(
                f"  computed generation curves for {len(generation_curves)} nodes"
            )
        else:
            median_gen_percent = pd.Series(dtype=float)
            gen_percent_overall = pd.Series(dtype=float)

        # Query 4: Median feature count (only rows with features)
        logger.info("Computing median feature count...")
        feature_data = pd.read_sql_query(
            """
            SELECT node_id,
                (SELECT COUNT(*) FROM json_each(rt.features)) as feature_count
            FROM runtime_test_case rt
            WHERE features != '{}'
            """,
            db._conn,
        )
        logger.info(f"  {len(feature_data)} rows with features")

        if not feature_data.empty:
            median_features = feature_data.groupby("node_id")["feature_count"].median()
        else:
            median_features = pd.Series(dtype=float)

        # Merge everything into sql_agg
        sql_agg = sql_agg.set_index("node_id")
        sql_agg["median_choices_size"] = median_choices
        sql_agg["median_execution_time"] = median_execution
        sql_agg["median_generation_percent"] = median_gen_percent
        sql_agg["generation_percent"] = gen_percent_overall
        sql_agg["generation_curve"] = generation_curves
        sql_agg["median_feature_count"] = median_features

        # Build result rows
        result_rows = []
        for node_id, row in sql_agg.iterrows():
            result_rows.append(
                {
                    "node_id": int(node_id),
                    "median_execution_time": row.get("median_execution_time"),
                    "median_generation_percent": row.get("median_generation_percent"),
                    "generation_percent": row.get("generation_percent"),
                    "execution_time_cv": row.get("execution_time_cv"),
                    "percent_overrun": row["percent_overrun"],
                    "percent_invalid": row["percent_invalid"],
                    "median_feature_count": row.get("median_feature_count"),
                    "min_choices_size": int(row["min_choices_size"]),
                    "median_choices_size": row.get("median_choices_size"),
                    "max_choices_size": int(row["max_choices_size"]),
                    "generation_curve": row.get("generation_curve"),
                }
            )

        logger.info(f"Computed metrics for {len(result_rows)} nodes")
        return {"rows": result_rows}

    @staticmethod
    def store_to_database(db: Any, data: dict[str, Any]):
        db.executemany(
            """
            INSERT OR REPLACE INTO node_aggregate_metrics (
                node_id, median_execution_time, median_generation_percent,
                generation_percent, execution_time_cv,
                percent_overrun, percent_invalid, median_feature_count,
                min_choices_size, median_choices_size, max_choices_size,
                generation_curve
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["node_id"],
                    row["median_execution_time"],
                    row["median_generation_percent"],
                    row["generation_percent"],
                    row["execution_time_cv"],
                    row["percent_overrun"],
                    row["percent_invalid"],
                    row["median_feature_count"],
                    row["min_choices_size"],
                    row["median_choices_size"],
                    row["max_choices_size"],
                    row["generation_curve"],
                )
                for row in data["rows"]
            ],
        )
        db.commit()
