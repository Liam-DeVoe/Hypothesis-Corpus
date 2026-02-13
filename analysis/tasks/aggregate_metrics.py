"""Pre-compute per-node aggregate metrics from runtime_testcase for fast
dashboard queries."""

import json
import logging
from typing import Any

import pandas as pd

from .task import Task

logger = logging.getLogger(__name__)


class AggregateMetricsTask(Task):
    name = "aggregate_metrics"
    follows = ["runtime"]

    @staticmethod
    def get_schema_sql() -> str:
        return """
            CREATE TABLE IF NOT EXISTS node_aggregate_metrics (
                node_id INTEGER PRIMARY KEY,
                median_exec_time REAL,
                median_generation_pct REAL,
                exec_time_cv REAL,
                pct_overrun REAL,
                pct_filtered REAL,
                median_feature_count REAL,
                min_choices_size INTEGER,
                median_choices_size REAL,
                max_choices_size INTEGER,
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
                SUM(CASE WHEN data_status = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_overrun,
                SUM(CASE WHEN data_status = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_filtered,
                MIN(choices_size) as min_choices_size,
                MAX(choices_size) as max_choices_size,
                AVG(exec_time) as mean_exec_time,
                AVG(exec_time * exec_time) - AVG(exec_time) * AVG(exec_time) as exec_time_variance,
                COUNT(exec_time) as exec_time_count
            FROM (
                SELECT
                    node_id,
                    data_status,
                    choices_size,
                    json_extract(timing, '$."execute:test"') as exec_time
                FROM runtime_testcase
            )
            GROUP BY node_id
            """,
            db._conn,
        )
        logger.info(f"  {len(sql_agg)} nodes from SQL aggregates")

        # Compute CV from variance
        sql_agg["exec_time_cv"] = None
        mask = (sql_agg["exec_time_count"] >= 10) & (sql_agg["mean_exec_time"] > 0)
        sql_agg.loc[mask, "exec_time_cv"] = (
            sql_agg.loc[mask, "exec_time_variance"].clip(lower=0) ** 0.5
            / sql_agg.loc[mask, "mean_exec_time"]
        )

        # Query 2: Medians for choices_size and exec_time
        # These are numeric columns, so the transfer is manageable (~15M rows × 3 cols)
        logger.info("Computing medians for choices_size and exec_time...")
        medians_data = pd.read_sql_query(
            """
            SELECT
                node_id,
                choices_size,
                json_extract(timing, '$."execute:test"') as exec_time
            FROM runtime_testcase
            """,
            db._conn,
        )
        logger.info(f"  loaded {len(medians_data)} rows")

        median_choices = medians_data.groupby("node_id")["choices_size"].median()
        median_exec = medians_data.groupby("node_id")["exec_time"].median()

        # Query 3: Median generation % (needs full timing JSON — one-time cost)
        logger.info("Computing median generation %...")
        timing_data = pd.read_sql_query(
            """
            SELECT node_id, timing
            FROM runtime_testcase
            """,
            db._conn,
        )
        logger.info(f"  loaded {len(timing_data)} rows")

        gen_rows = []
        for _, row in timing_data.iterrows():
            timing = json.loads(row["timing"])
            exec_time = timing.get("execute:test", 0)
            gen_time = sum(v for k, v in timing.items() if k.startswith("generate:"))
            total = exec_time + gen_time
            if total > 0:
                gen_rows.append(
                    {"node_id": row["node_id"], "gen_pct": gen_time / total * 100}
                )

        if gen_rows:
            gen_df = pd.DataFrame(gen_rows)
            median_gen_pct = gen_df.groupby("node_id")["gen_pct"].median()
        else:
            median_gen_pct = pd.Series(dtype=float)

        # Query 4: Median feature count (only rows with features)
        logger.info("Computing median feature count...")
        feature_data = pd.read_sql_query(
            """
            SELECT node_id,
                (SELECT COUNT(*) FROM json_each(rt.features)) as feature_count
            FROM runtime_testcase rt
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
        sql_agg["median_exec_time"] = median_exec
        sql_agg["median_generation_pct"] = median_gen_pct
        sql_agg["median_feature_count"] = median_features

        # Build result rows
        result_rows = []
        for node_id, row in sql_agg.iterrows():
            result_rows.append(
                {
                    "node_id": int(node_id),
                    "median_exec_time": row.get("median_exec_time"),
                    "median_generation_pct": row.get("median_generation_pct"),
                    "exec_time_cv": row.get("exec_time_cv"),
                    "pct_overrun": row["pct_overrun"],
                    "pct_filtered": row["pct_filtered"],
                    "median_feature_count": row.get("median_feature_count"),
                    "min_choices_size": int(row["min_choices_size"]),
                    "median_choices_size": row.get("median_choices_size"),
                    "max_choices_size": int(row["max_choices_size"]),
                }
            )

        logger.info(f"Computed metrics for {len(result_rows)} nodes")
        return {"rows": result_rows}

    @staticmethod
    def store_to_database(db: Any, data: dict[str, Any]):
        db.executemany(
            """
            INSERT OR REPLACE INTO node_aggregate_metrics (
                node_id, median_exec_time, median_generation_pct, exec_time_cv,
                pct_overrun, pct_filtered, median_feature_count,
                min_choices_size, median_choices_size, max_choices_size
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["node_id"],
                    row["median_exec_time"],
                    row["median_generation_pct"],
                    row["exec_time_cv"],
                    row["pct_overrun"],
                    row["pct_filtered"],
                    row["median_feature_count"],
                    row["min_choices_size"],
                    row["median_choices_size"],
                    row["max_choices_size"],
                )
                for row in data["rows"]
            ],
        )
        db.commit()

    @staticmethod
    def delete_data(db: Any):
        db.execute("DELETE FROM node_aggregate_metrics")
        db.commit()
