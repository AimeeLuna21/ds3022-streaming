# Coinbase Trades: Comparing USD Trade Value Across Product Pairs

**Author:** Aimee Salazar Luna  
**DS 3022 Data Project 3, Fall 2026**

## **Data source**

I collected public Coinbase Exchange trade messages from `wss://ws-feed.exchange.coinbase.com` using a WebSocket, which pushes messages to the producer. I subscribed to the `matches` channel for six USD product pairs: BTC-USD, ETH-USD, SOL-USD, XRP-USD, ADA-USD, and DOGE-USD. A trade message includes the product pair, trade ID, side, size, price, sequence number, and event time. No account or API key is needed. The connection can send a subscription confirmation as well as trade messages, so the producer skips messages that do not contain trade fields.


## How to run
From a fresh clone, install the dependencies and start the Kafka-compatible Redpanda cluster:

```bash
cd ds3022-streaming
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
docker compose up -d
docker exec -it redpanda-0 rpk topic create coinbase_trades -p 3 -r 3
```

Open two terminals from the repository folder. In the first terminal, start the producer:
```bash
cd ds3022-streaming/project3
source ../.venv/bin/activate
python producer.py
```

In the second terminal, start the lander:
```bash
cd ds3022-streaming/project3
source ../.venv/bin/activate
python lander.py
```

Leave both running while collecting data. To run the full-data flow after records have been landed, start Prefect in another terminal:
```bash
cd ds3022-streaming/project3
source ../.venv/bin/activate
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start
```

Then, in another terminal, run the flow and the sample flow:
```bash
cd ds3022-streaming/project3
source ../.venv/bin/activate
python flow.py
python kit/make_sample.py
python flow.py data/sample
```


## Architecture
producer.py → Kafka topic coinbase_trades → lander.py → raw Parquet files in data/raw/ → flow.py tasks (load_raw → stage → entities → marts → chart) → DuckDB tables, including mart_volume_by_product → charts/volume_by_product.png


## Design decisions

1. **Key and partitions.** I used `product_id` as the Kafka key and created three partitions. This keeps trades for each product pair in the same partition and preserves their order; using `trade_id` as the key would spread a pair’s trades across partitions and lose that ordering.

2. **Batch size and time.** The lander writes a batch at 1,000 records or after 60 seconds. My probe measured about 20.68 messages per second, which would fill 1,000 records in about 48 seconds; at that rate, the size limit would trigger first and make about 74 files per hour. During my actual capture, the rate varied, and many batches reached the 60-second limit before reaching 1,000 records.

3. **Offsets after the file.** The lander writes and renames the Parquet file before storing Kafka offsets. If it crashes between those steps, Kafka sends those messages again and the raw folder can contain duplicate records; I accept this at-least-once behavior because the flow removes duplicates later.

4. **Duplicates.** I identify the same Kafka record by its partition and offset, and the staging step keeps one copy of each. I identify the same Coinbase trade by `product_id` and `trade_id`; the entities step keeps one version of each trade.

5. **Windows and time.** I keep Coinbase’s `event_time` as the event-time column, but this chart does not use a time window. `mart_volume_by_product` compares total trade value across the full capture because my question asks which product pair had the highest overall total.

6. **Ten times the volume.** At ten times the volume, scanning all raw Parquet files again in the flow and rebuilding the chart could take longer, and the raw folder would grow faster. I would increase the batch size, organize raw files by date, and make the flow process new files incrementally.

## Chaos round

| Card | Prediction | What I did | Numbers before → after | Explanation |
|---|---|---|---|---|
| D1 Crash between file and offsets | I predict raw will increase by the new trades plus one repeated 1,000-record batch. Unique Kafka records and unique trades will increase only by the new trades because the crash happens after the file is written but before offsets are stored. | Ran `python producer.py` for about 2 minutes and stopped it; ran `python lander.py crash`, then restarted with `python lander.py`; ran `python four_numbers.py product_id,trade_id`. | Raw 52,446 → 57,472; malformed 0 → 0; unique Kafka records 52,446 → 56,472; unique product_id+trade_id 52,445 → 56,471 | The crash wrote a 1,000-record file but did not store offsets. The restart read and wrote those same records again. This is at-least-once delivery: raw contains the repeated batch, and the later flow can deduplicate it. |
| D2 Poison message | I predict raw and malformed will each increase by 1; unique Kafka records and unique trades will stay the same. | Ran `docker exec -it redpanda-0 rpk topic produce coinbase_trades -k bad`, typed `this is not JSON`, pressed Enter and Control+C, waited for the lander to print `Landed 1 records`, then ran `python four_numbers.py product_id,trade_id`. | Raw 57,472 → 57,473; malformed 0 → 1; unique Kafka records 56,472 → 56,472; unique product_id+trade_id 56,471 → 56,471 | The lander kept running and saved the invalid JSON as malformed instead of crashing. The bad message has no trade identity, so it does not change the unique record or trade counts. |
| D4 Source drops | I predict a brief network drop will pause the producer, which will reconnect when Wi-Fi returns. New records received after reconnect should not be duplicated. | Ran `python producer.py`, turned Wi-Fi off for 30 seconds and back on, stopped the producer after it reconnected, waited for the lander to write a file, then ran `python four_numbers.py product_id,trade_id`. | Raw 57,473 → 58,008; malformed 1 → 1; unique Kafka records 56,472 → 57,007; unique product_id+trade_id 56,471 → 57,006 | Raw, unique Kafka records, and unique trades each increased by 535; malformed stayed at 1. The reconnect added no duplicate rows. |


## The four numbers

From the flow log on the full data (screenshot in `docs/`):

| Number | Full data | Sample (`data/sample`) |
|---|---:|---:|
| Raw records landed | 58,022 | 5,000 |
| Malformed (kept in raw) | 1 | 0 |
| Unique Kafka records | 56,978 | 4,989 |
| Unique trades | 56,978 | 4,989 |

## Challenges / Obstacles
Coinbase’s live message rate changed over time, and the WebSocket disconnected during my reconnect test. I also found that its trade messages could arrive as `match`, while my first lander expected `last_match`, so valid trades were initially marked malformed. I inspected the raw Parquet records and updated the lander to recognize both message types while keeping invalid JSON in the raw data. I used the course probe, producer logs, Redpanda Console, DuckDB, and Prefect to check the source, Kafka topic, landed records, and flow results.

## Analysis

I asked which Coinbase USD product pair had the highest total trade value in my capture and how the other pairs compared. Using the full-capture comparison in `mart_volume_by_product`, BTC-USD had the highest observed total at $19.29 million, followed by ETH-USD at $10.85 million; SOL-USD and XRP-USD followed at $5.08 million and $3.77 million. BTC-USD’s total was about 1.78 times ETH-USD’s, which surprised me. This is a limited live capture of six selected pairs, and the Coinbase feed can drop messages, so these totals may not represent all Coinbase trading.

## Plot / Visualization

![BTC-USD had the highest USD trade value](charts/volume_by_product.png)

## Reflection
Before this project, I thought of Kafka mainly as a place to send messages. I learned that streaming also requires tracking partitions and offsets, landing records safely, and planning for messages to appear again after a crash. My D1 experiment made at-least-once delivery concrete: the lander wrote a file before storing offsets, then replayed records when it restarted. I also learned to keep raw data when a message is malformed and handle filtering and deduplication in later steps. The source needed attention too: Coinbase sent a subscription notice, its message types did not all match my first assumptions, and the WebSocket disconnected and reconnected. I used the probe, producer logs, Redpanda Console, Parquet counts, DuckDB, and Prefect to check each stage. If I started again, I would inspect and test real source messages with the lander before collecting a large volume. I would also update my progress log as I completed each checkpoint.

## Acknowledgements

I used the course-provided `probe.py` and its Coinbase WebSocket connection example as a starting point for my producer. I used ChatGPT to help me understand the worksheet, adapt the producer to send Coinbase trade messages to Kafka, and prepare this source card. I checked the source details and measured rate against my `probe.py` output, and checked the producer output in Redpanda Console by confirming that the `coinbase_trades` topic received records across all three partitions.
