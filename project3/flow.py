"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"coinbase{TAG}.duckdb"
CHART = f"charts/volume_by_product{TAG}.png"


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"""
            CREATE OR REPLACE TABLE raw_trades AS
            SELECT *,
                   json_extract_string(try_cast(raw AS JSON), '$.data.type') AS msg_type
            FROM read_parquet('{RAW}')
        """)
        return con.execute("SELECT count(*) FROM raw_trades").fetchone()[0]


@task
def stage():
    """Keep one copy of valid Coinbase match messages."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_trades
            WHERE action <> 'malformed'
              AND msg_type = 'match'
              AND product_id IS NOT NULL
              AND trade_id IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {
            "malformed": q("SELECT count(*) FROM raw_trades WHERE action = 'malformed'"),
            "records": q("SELECT count(*) FROM stg_records"),
        }


@task
def entities():
    """Keep one row per product_id and trade_id."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE trades AS
            SELECT product_id, trade_id, side, price, size,
                   CAST(event_time AS TIMESTAMP) AS event_time,
                   CAST(raw::JSON ->> '$.data.sequence' AS BIGINT) AS sequence
            FROM stg_records
            QUALIFY row_number() OVER (
                PARTITION BY product_id, trade_id
                ORDER BY kafka_offset DESC
            ) = 1
        """)
        return con.execute("SELECT count(*) FROM trades").fetchone()[0]


@task
def marts():
    """Summarize total USD trade value for each Coinbase product pair."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_volume_by_product AS
            SELECT product_id,
                   count(*) AS trade_count,
                   round(sum(price * size), 2) AS notional_usd
            FROM trades
            GROUP BY product_id
            ORDER BY notional_usd DESC
        """)
        return con.execute("""
            SELECT product_id, trade_count, notional_usd
            FROM mart_volume_by_product
            ORDER BY notional_usd DESC
        """).fetchall()


@task
def marts():
    """Summarize total USD trade value for each Coinbase product pair."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_volume_by_product AS
            SELECT product_id,
                   count(*) AS trade_count,
                   round(sum(price * size), 2) AS notional_usd
            FROM trades
            GROUP BY product_id
            ORDER BY notional_usd DESC
        """)
        return con.execute("""
            SELECT product_id, trade_count, notional_usd
            FROM mart_volume_by_product
            ORDER BY notional_usd DESC
        """).fetchall()


@task
def chart():
    """Compare total USD trade value across product pairs."""
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT product_id, notional_usd
            FROM mart_volume_by_product
            ORDER BY notional_usd DESC
        """).fetchall()

    if not rows:
        raise ValueError("mart_volume_by_product has no rows to plot")

    os.makedirs("charts", exist_ok=True)
    # Reverse the order so the largest bar appears at the top.
    products = [row[0] for row in rows][::-1]
    values = [row[1] for row in rows][::-1]
    leader = rows[0][0]

    plt.figure(figsize=(8, 4))
    plt.barh(products, values, color="#2a7fb8")
    plt.title(f"{leader} had the highest USD trade value")
    plt.xlabel("Total trade value (USD)")
    plt.ylabel("Coinbase product pair")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    plt.close()
    return CHART


@flow(name="coinbase-pipeline")
def coinbase_pipeline():
    log = get_run_logger()

    # Run each task in pipeline order.
    raw = load_raw()
    n = stage()
    trades = entities()
    marts()
    path = chart()

    # These are the four counts the worksheet asks us to report.
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", n["malformed"])
    log.info("Unique Kafka records:      %d", n["records"])
    log.info("Unique trades:             %d", trades)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    coinbase_pipeline()
