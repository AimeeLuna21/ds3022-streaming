"""Kafka topic "coinbase_trades" -> raw Parquet files in data/raw/.

A batch is written when it has 1000 records or is 60 seconds old.
Offsets are stored only AFTER the file exists: at-least-once.

  python lander.py
  python lander.py crash   # experiment: crash after the first file, before storing offsets
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from quixstreams import Application

# Connect to the local Kafka broker and listen to our Coinbase topic.
BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"

# Store Parquet batches in project3/data/raw when run from the project3 folder.
OUT_DIR = "data/raw"

# Keep the same batch rules as the original class lander.
BATCH_SIZE = 1000
BATCH_SECONDS = 60

# This is for the later crash experiment. Don't add "crash" when running normally.
CRASH_AFTER_WRITE = "crash" in sys.argv[1:]

# The first three columns identify the Kafka record.
# The next fields describe a Coinbase trade. "raw" keeps the entire original message.
SCHEMA = """kafka_partition INTEGER, kafka_offset BIGINT, kafka_key VARCHAR,
    action VARCHAR, product_id VARCHAR, trade_id VARCHAR, side VARCHAR,
    size DOUBLE, price DOUBLE, sequence BIGINT, event_time VARCHAR,
    landed_at TIMESTAMP, raw VARCHAR"""


def number(x):
    """Turn numeric text, such as a Coinbase price, into a number."""
    try:
        return float(x)
    except (TypeError, ValueError):
        # Missing or invalid values become NULL in DuckDB instead of stopping the lander.
        return None


def integer(x):
    """Turn the Coinbase sequence number into an integer."""
    try:
        return int(x)
    except (TypeError, ValueError):
        # Keep missing or invalid sequence numbers as NULL.
        return None


def to_row(msg):
    """Keep the whole message (raw) plus useful Kafka and Coinbase columns."""

    # Decode the original Kafka value. Keep it so we don't lose any source fields.
    raw = msg.value().decode("utf8", errors="replace") if msg.value() else ""

    try:
        # Our producer wraps each Coinbase message as {"action": ..., "data": ...}.
        m = json.loads(raw)
        data = m["data"]
        action = m.get("action", "unknown")

        # Coinbase sends this notice when the WebSocket subscription is set up.
        # Keep it in raw data, but don't count it as a trade.
        if data.get("type") == "subscriptions":
            action = "control"

        # A trade should have type last_match and include its product and trade ID.
        elif (
            data.get("type") != "last_match"
            or data.get("product_id") is None
            or data.get("trade_id") is None
        ):
            action = "malformed"

    except (ValueError, KeyError, TypeError, AttributeError):
        # Keep malformed messages in raw instead of crashing the consumer.
        data, action = {}, "malformed"

    # Decode the Kafka key. Our producer uses product_id as the key.
    key = msg.key().decode("utf8", errors="replace") if msg.key() else None

    # Return values in the same order as the columns in SCHEMA.
    return (
        msg.partition(),       # Kafka partition containing this message
        msg.offset(),          # Message's position within that partition
        key,                   # product_id, which keeps each product ordered
        action,                # "live" for trades; "control" or "malformed" otherwise
        data.get("product_id"),
        str(data["trade_id"]) if data.get("trade_id") is not None else None,
        data.get("side"),
        number(data.get("size")),
        number(data.get("price")),
        integer(data.get("sequence")),
        data.get("time"),      # Coinbase event time
        datetime.now(timezone.utc).replace(tzinfo=None),  # When we landed the message
        raw,                   # The complete original Kafka value
    )


def write_batch(rows):
    """Write one Parquet file. Write to .tmp first, then rename,
    so the flow never reads a half-written file."""

    # Create data/raw if this is the first batch.
    os.makedirs(OUT_DIR, exist_ok=True)

    # Use the time and a short random ID to make each filename unique.
    name = (
        f"{OUT_DIR}/coinbase_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_"
        f"{uuid.uuid4().hex[:6]}.parquet"
    )

    # DuckDB is used in memory to create this batch; it makes no persistent .duckdb file.
    con = duckdb.connect()
    con.execute(f"CREATE TABLE batch ({SCHEMA})")

    # There are 13 columns in the Coinbase schema, so each row needs 13 placeholders.
    placeholders = ", ".join(["?"] * 13)
    con.executemany(f"INSERT INTO batch VALUES ({placeholders})", rows)

    # Write to a temporary filename first.
    con.execute(f"COPY batch TO '{name}.tmp' (FORMAT parquet)")
    con.close()

    # Rename only after the temporary Parquet file is complete.
    os.replace(f"{name}.tmp", name)
    return name


def main():
    # Use a Coinbase-specific consumer group so its saved offsets are separate
    # from the earthquake demo's quakes_lander group.
    app = Application(
        broker_address=BROKER,
        consumer_group="coinbase_trades_lander",
        auto_offset_reset="earliest",
        loglevel="WARNING",
    )

    with app.get_consumer() as consumer:
        # Subscribe to the Coinbase topic.
        consumer.subscribe([TOPIC])

        # rows holds this batch; last tracks the newest message in each partition.
        rows, last, started = [], {}, time.monotonic()

        print("Landing records from", TOPIC, "into", OUT_DIR, "(Ctrl+C to stop)")

        while True:
            # Wait up to one second for a message.
            msg = consumer.poll(1)

            if msg is not None:
                # Stop if Kafka reports an error.
                if msg.error() is not None:
                    raise Exception(msg.error())

                # Start the timer when the first message enters an empty batch.
                if not rows:
                    started = time.monotonic()

                # Convert the message to a row and add it to the current batch.
                rows.append(to_row(msg))

                # Remember the newest message from this partition.
                last[msg.partition()] = msg

            # Write when the batch reaches 1000 messages...
            full = len(rows) >= BATCH_SIZE

            # ...or when a nonempty batch has waited 60 seconds.
            old = rows and time.monotonic() - started >= BATCH_SECONDS

            if full or old:
                # Finish the Parquet file before saving offsets.
                path = write_batch(rows)
                print(f"Landed {len(rows):4d} records -> {path}")

                # This deliberately simulates a crash for the later D1 exercise.
                # The file exists, but Kafka offsets have not been stored yet.
                if CRASH_AFTER_WRITE:
                    print("CRASH (on purpose): the file exists, the offsets were NOT stored")
                    os._exit(1)

                # Save offsets only after the file exists.
                # A crash before this point means Kafka can send those messages again.
                for m in last.values():
                    consumer.store_offsets(message=m)

                # Empty the batch and start collecting the next one.
                rows, last = [], {}


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # Any unfinished batch has no saved offsets, so Kafka can send it again next time.
        pass
