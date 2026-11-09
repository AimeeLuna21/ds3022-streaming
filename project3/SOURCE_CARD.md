# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

**Name:** Aimee Salazar Luna
**Date:** 09 Nov 2026

## 1. The source
- Source and URL: Coinbase Exchange public market data, wss://ws-feed.exchange.coinbase.com
- Method: WebSocket push; subscribe to the matches channel.
- Account, key or User-Agent needed: No account or API key. Subscribe within 5 seconds of connecting.
- Limits I know about: Rate varies with market activity. There is no replay on the WebSocket, and Coinbase warns that messages can be dropped. The initial subscriptions message is not a trade and has no product_id.

## 2. One real message
    {
      "type": "last_match",
      "trade_id": 256691194,
      "maker_order_id": "f216575c-e946-4368-9945-00f96760494c",
      "taker_order_id": "6a060530-b832-4129-9f8f-bb772e78a182",
      "side": "sell",
      "size": "0.000005",
      "price": "1.0928",
      "product_id": "XRP-USD",
      "sequence": 23284591079,
      "time": "2026-11-09T14:02:16.809917Z"
    }

## 3. Volume maths
- Measured rate: 20.68 messages per second
- Live capture needed for 50,000: about 2,418 seconds, or 40.3 minutes, at the measured rate
- History / backfill available? No replay; collect live messages.
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Run the producer for at least 90 minutes before Session 3, check the topic count, and collect longer if it is below 50,000. At the measured rate, 90 minutes would yield about 111,672 messages.

## 4. Topic design
- Topic name: coinbase_trades
- Kafka key: product_id
- Why this key: Trades for each product stay in order, while the different products can be distributed across the three partitions.

## 5. Duplicates
- Same Kafka record: The same topic, partition, and offset; the lander should not count it twice.
- Same thing: The same trade, identified by product_id + trade_id.
- Which version to keep? Keep one record per product_id + trade_id; a trade is an individual event.

## 6. Research question (draft)
- How do hourly trade counts and buy-versus-sell volume compare across products, and do changes in volume occur alongside price changes?

## 7. Self-check (checkpoint A)
Tick these only after reviewing the card and confirming each statement is true.

- [ ] I ran the probe for this source today and messages arrived.
- [ ] The rate in section 3 is measured, not guessed.
- [ ] My volume plan reaches 50,000 raw records before Session 3, with margin.
- [ ] It needs no API key or account that takes days to get.
- [ ] My key has a reason that fits my question.
- [ ] I named both kinds of duplicates for my source.
- [ ] My draft question can be answered with a time window, comparison, anomaly or join.
