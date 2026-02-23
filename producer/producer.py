import os
import csv
import json
import time
import random
import glob
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-0:9092').split(',')
TOPIC = os.getenv('KAFKA_TOPIC', 'stock-market')
DATA_PATH = os.getenv('DATA_PATH', '/data')
DELAY_MIN = float(os.getenv('DELAY_MIN'))
DELAY_MAX = float(os.getenv('DELAY_MAX'))
CLIENT_ID = os.getenv('CLIENT_ID', 'stock-producer')


class Producer:
    def _find_all_csv_files(self, root_dir):
        """find all csv data files"""
        csv_files = []
        for dirpath, _, filenames in os.walk(root_dir):
            for f in filenames:
                if f.lower().endswith('.csv'):
                    full_path = os.path.join(dirpath, f)
                    csv_files.append(full_path)
        if not csv_files:
            raise Exception(f"No CSV files found in {root_dir}")
            return csv_files
        return csv_files

    def _create_producer(self):
        """Create connection to Kafka"""
        for attempt in range(10):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    client_id=CLIENT_ID,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Connected to Kafka")
                return producer
            except NoBrokersAvailable:
                print(f"Brokers not available, retrying ({attempt+1}/10)...")
                time.sleep(5)
        raise Exception("Could not connect to Kafka after 10 attempts")

    def __init__(self):
        self.kafka = self._create_producer()
        self.data_files = self._find_all_csv_files(DATA_PATH)
        print(f"Found {len(self.data_files)} CSV files in total:")


def stream_csv(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def main():
    try:
        producer = Producer()
        random.shuffle(producer.data_files)
        for csv_file in producer.data_files:
            relative_path = os.path.relpath(csv_file, DATA_PATH)
            ticker_name = os.path.splitext(os.path.basename(csv_file))[0]
            print(f"Processing file: {relative_path} (ticker: {ticker_name})")
            for row in stream_csv(csv_file):
                message = {
                    'ticker': ticker_name,
                    'file': relative_path,
                    **row
                }
                # Send in Kafka
                producer.kafka.send(TOPIC, value=message)
                print(f"Sent: {ticker_name} {list(row.values())[:3]}...")
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                time.sleep(delay)
            print(f"Finished file {relative_path}")
        print("All files processed. Restarting loop...")
        producer.flush()
        producer.close()
    except KeyboardInterrupt:
        print("Stopping producer...")

if __name__ == '__main__':
    main()
