# TODO: your project title

**Author:** TODO
**DS 3022 Data Project 3, Fall 2026**

> There is no interview for this project: this README is where you explain and defend your pipeline. Fill in the title, your name and every italic placeholder, then delete this note. Keep the headings: `kit/check_project.py` looks for them.

## Data source

_(replace: what source, which URL or stream, pull or push, what one message contains. Mention any account, key or User-Agent needed. If you replay an archive rather than stream live, say so.)_

## How to run

_(replace: the exact commands, in order, from a fresh clone. A classmate will follow them in Session 4 (Mon 16 Nov) without asking you. Include: virtual environment and `pip install`, starting the cluster, creating the topic, running the producer and lander, running the flow on the full data and on the sample:)_

```bash
cd project3
python flow.py data/sample      # runs without Kafka, on the committed sample
```

## Architecture

_(replace: a diagram (an image in docs/ or a text diagram) of producer → topic → lander → raw Parquet → flow tasks → tables → charts, with the names of your files, topic, tables and charts.)_

## Design decisions

Answer each in 2-4 sentences, in your own words, with your own numbers.

1. **Key and partitions.** _(replace: which key, why, and what it guarantees about order. What would go wrong with another key?)_
2. **Batch size and time.** _(replace: your batch size and time limit. With your source's rate, which rule closes most batches? How many files per hour does that make?)_
3. **Offsets after the file.** _(replace: what happens if the lander crashes after writing a file but before storing offsets, and why you accept that.)_
4. **Duplicates.** _(replace: what makes two records the same Kafka record, and what makes them the same thing. Which SQL removes each kind?)_
5. **Windows and time.** _(replace: which time column your analysis uses (event time or landing time), which window size, and why.)_
6. **Ten times the volume.** _(replace: what in your pipeline would break or slow down first at 10x your rate, and what you would change.)_

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
| Raw records landed | TODO | TODO |
| Malformed (kept in raw) | TODO | TODO |
| Unique Kafka records | TODO | TODO |
| Unique TODO (your entity) | TODO | TODO |

## Challenges / Obstacles

_(replace: what this source made hard in gathering, processing and analysis, and how you worked through it. What methods and tools did you use?)_

## Analysis

_(replace: one clear paragraph. Your research question, what the data shows, one finding that surprised you or is not obvious, and one limitation of your data or method.)_

## Plot / Visualization

_(replace: at least one chart, saved in charts/ and shown here. Give it a title and labelled axes with units.)_

![TODO chart title](charts/TODO.png)

## Reflection

_(replace: about 150 words, first person. What did you learn about streaming systems that you did not know before Thu 22 Oct? What would you do differently if you started again?)_

## Acknowledgements

I used the course-provided `probe.py` and its Coinbase WebSocket connection example as a starting point for my producer. I used ChatGPT to help me understand the worksheet, adapt the producer to send Coinbase trade messages to Kafka, and prepare this source card. I checked the source details and measured rate against my `probe.py` output, and checked the producer output in Redpanda Console by confirming that the `coinbase_trades` topic received records across all three partitions.
