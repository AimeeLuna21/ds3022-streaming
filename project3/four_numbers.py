"""Count raw rows, malformed messages, Kafka records, and unique trades."""

import sys
import duckdb

# Show how to run the script if the trade identity columns are not supplied.
USAGE = "Usage: python four_numbers.py product_id,trade_id"

# Each raw Parquet row is one landed Kafka message.
RAW = "read_parquet('data/raw/*.parquet')"

# Exclude malformed records from the valid Kafka-record and trade counts.
OK = "action <> 'malformed'"

# Count malformed rows separately; they remain preserved in the raw files.
BAD = "count(*) FILTER (WHERE action = 'malformed')"

# A Kafka record is uniquely identified by its partition and offset.
KEYS = (
    f"SELECT DISTINCT kafka_partition, kafka_offset "
    f"FROM {RAW} WHERE {OK}"
)

# A Coinbase trade is identified by product_id plus trade_id.
IDS = sys.argv[1].split(",") if len(sys.argv) > 1 else sys.exit(USAGE)
HAS_ID = " AND ".join(f"{column} IS NOT NULL" for column in IDS)

# Count unique trade identities after excluding malformed rows.
THINGS = (
    f"SELECT DISTINCT {', '.join(IDS)} "
    f"FROM {RAW} WHERE {OK} AND {HAS_ID}"
)

# Connect to DuckDB in memory; this script reads files and writes nothing.
query = duckdb.connect().execute

# First two numbers: every raw row and the malformed subset.
raw_count, malformed_count = query(
    f"SELECT count(*), {BAD} FROM {RAW}"
).fetchone()

# Third number: distinct Kafka partition/offset pairs.
unique_kafka_count = query(
    f"SELECT count(*) FROM ({KEYS})"
).fetchone()[0]

# Fourth number: distinct product_id/trade_id pairs.
unique_trade_count = query(
    f"SELECT count(*) FROM ({THINGS})"
).fetchone()[0]

print(
    f"raw {raw_count:,} | malformed {malformed_count:,} "
    f"| unique Kafka records {unique_kafka_count:,} "
    f"| unique {'+'.join(IDS)} {unique_trade_count:,}"
)
