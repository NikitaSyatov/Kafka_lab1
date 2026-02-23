#!/usr/bin/env python3
import os
import json
import logging
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('data-processing')

# Configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-0:9092').split(',')
INPUT_TOPIC = os.getenv('KAFKA_TOPIC', 'stock-market')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'data-processor-group')
WINDOW_SIZE = int(os.getenv('WINDOW_SIZE', '10'))
ML_TOPIC = os.getenv('ML_TOPIC', 'ml-input')
ML_BOOTSTRAP_SERVERS = os.getenv('ML_BOOTSTRAP_SERVERS', 'kafka-0:9092').split(',')

class Consumer:
    def _create_consumer():
        for attempt in range(10):
            try:
                consumer = KafkaConsumer(
                    INPUT_TOPIC,
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    group_id=GROUP_ID,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                logger.info("Connected to Kafka (consumer)")
                return consumer
            except NoBrokersAvailable:
                logger.warning(f"No brokers available, retrying ({attempt+1}/10)...")
                time.sleep(5)
        raise Exception("Could not connect to Kafka after 10 attempts")

    def __init__(self):
        self.kafka = self._create_consumer()


class Producer:
    def _create_producer():
        for attempt in range(10):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=ML_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("Connected to Kafka (producer)")
                return producer
            except NoBrokersAvailable:
                logger.warning(f"No brokers available for producer, retrying ({attempt+1}/10)...")
                time.sleep(5)
        raise Exception("Could not connect to Kafka producer after 10 attempts")

    def __init__(self):
        self.kafka = self._create_producer()

def process_message(message, producer):
    data = message.value
    ticker = data.get('ticker')
    close_price = data.get('Close') or data.get('close')
    price_windows = {}

    if close_price is None:
        logger.debug(f"No close price in message: {data}")
        return
    close_price = float(close_price)

    if ticker not in price_windows:
        price_windows[ticker] = deque(maxlen=WINDOW_SIZE)
    price_windows[ticker].append(close_price)

    avg_price = sum(price_windows[ticker]) / len(price_windows[ticker])
    logger.info(f"[PROCESSED] {ticker}: current close={close_price:.2f}, moving avg ({WINDOW_SIZE})={avg_price:.2f}")

    # Формируем сообщение для ML топика
    ml_message = {
        'ticker': ticker,
        'close_price': close_price,
        'moving_avg': avg_price,
        'timestamp': time.time()
    }
    producer.send(ML_TOPIC, value=ml_message)
    logger.debug(f"Sent to {ML_TOPIC}: {ml_message}")

def main():
    logger.info("Starting data processing consumer")
    consumer = create_consumer()
    producer = create_producer()

    try:
        for msg in consumer:
            process_message(msg, producer)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == '__main__':
    main()