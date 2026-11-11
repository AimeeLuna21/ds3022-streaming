"""Coinbase live trades -> Kafka topic coinbase_trades."""

import asyncio
import json
import logging

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "coinbase_trades"
WSS_URL = "wss://ws-feed.exchange.coinbase.com"

SUBSCRIBE = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"],
    "channels": ["matches"],
}


def send(producer, data):
    """Send one trade, keyed by product so each product stays in order."""
    product_id = data.get("product_id")
    if not product_id or "trade_id" not in data:
        logging.warning("Skipped a message without trade fields")
        return False

    envelope = {"action": "live", "data": data}
    producer.produce(topic=TOPIC, key=product_id, value=json.dumps(envelope))
    return True


async def live(producer):
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            await ws.write_message(json.dumps(SUBSCRIBE))
            logging.info("Connected and subscribed to Coinbase matches")
            sent = 0

            while True:
                message_text = await ws.read_message()
                if message_text is None:
                    logging.warning("Coinbase closed the connection")
                    break

                try:
                    data = json.loads(message_text)
                    if send(producer, data):
                        sent += 1
                        if sent % 1000 == 0:
                            logging.info("%d trades sent to %s", sent, TOPIC)
                except (ValueError, TypeError) as error:
                    logging.warning("Skipped a message I could not read: %r", error)

        except Exception as error:
            logging.warning("Connection failed: %r", error)

        logging.info("Reconnecting in 5 seconds")
        await asyncio.sleep(5)


def main():
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        main()
    except KeyboardInterrupt:
        logging.info("Producer stopped")
