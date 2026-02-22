#!/usr/bin/env python3
import os
import json
import logging
from collections import defaultdict, deque
from kafka import KafkaConsumer, KafkaProducer
import time
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ml-consumer')

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-0:9092,kafka-1:9092').split(',')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'ml-input')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'ml-result')
GROUP_ID = os.getenv('GROUP_ID', 'ml-group')
WINDOW_SIZE = int(os.getenv('WINDOW_SIZE', '10'))

price_history = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

def predict_next(ticker):
    history = list(price_history[ticker])
    if len(history) < 2:
        return None
    x = np.arange(len(history))
    y = np.array(history)
    A = np.vstack([x, np.ones(len(x))]).T
    try:
        m, c = np.linalg.lstsq(A, y, rcond=None)[0]
        next_val = m * len(history) + c
        return next_val
    except:
        return None

def main():
    logger.info("Starting ML consumer")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logger.info(f"Subscribed to {INPUT_TOPIC}, producing to {OUTPUT_TOPIC}")

    try:
        for msg in consumer:
            data = msg.value
            ticker = data.get('ticker')
            close_price = data.get('close_price')
            if not ticker or close_price is None:
                logger.debug(f"Invalid message: {data}")
                continue
            close_price = float(close_price)
            price_history[ticker].append(close_price)
            logger.info(f"Received {ticker}: {close_price}")
            pred = predict_next(ticker)
            if pred is not None:
                logger.info(f"🔮 PREDICTION for {ticker}: next close = {pred:.2f}")
                result_message = {
                    'ticker': ticker,
                    'predicted_close': round(pred, 2),
                    'based_on_last': len(price_history[ticker]),
                    'timestamp': time.time()
                }
                producer.send(OUTPUT_TOPIC, value=result_message)
            else:
                logger.info(f"Not enough data to predict for {ticker} (need at least 2)")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == '__main__':
    main()