import os
import csv
import json
import time
import random
import glob
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Конфигурация из переменных окружения
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-0:9092').split(',')
TOPIC = os.getenv('KAFKA_TOPIC', 'stock-market')
DATA_PATH = os.getenv('DATA_PATH', '/data')
DELAY_MIN = int(os.getenv('DELAY_MIN', '1'))
DELAY_MAX = int(os.getenv('DELAY_MAX', '5'))
CLIENT_ID = os.getenv('CLIENT_ID', 'stock-producer')

def find_all_csv_files(root_dir):
    """Рекурсивно ищет все CSV-файлы в root_dir и возвращает список полных путей."""
    csv_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            if f.lower().endswith('.csv'):
                full_path = os.path.join(dirpath, f)
                csv_files.append(full_path)
    return csv_files

# При старте сканируем один раз (можно и каждый цикл, но для производительности так)
all_csv_files = find_all_csv_files(DATA_PATH)
if not all_csv_files:
    raise Exception(f"No CSV files found in {DATA_PATH}")

print(f"Found {len(all_csv_files)} CSV files in total:")
for f in all_csv_files[:5]:  # покажем первые 5
    print(f"  {f}")
if len(all_csv_files) > 5:
    print(f"  ... and {len(all_csv_files)-5} more")

def create_producer():
    """Создаёт подключение к Kafka с повторными попытками."""
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

producer = create_producer()

def stream_csv(file_path):
    """Генератор, построчно читающий CSV и возвращающий словари."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

try:
    while True:
        # Перемешиваем список файлов, чтобы продюсеры не всегда шли в одном порядке
        random.shuffle(all_csv_files)
        for csv_file in all_csv_files:
            # Извлекаем относительный путь от DATA_PATH для информативности
            rel_path = os.path.relpath(csv_file, DATA_PATH)
            # Имя тикера — имя файла без расширения
            ticker = os.path.splitext(os.path.basename(csv_file))[0]
            print(f"Processing file: {rel_path} (ticker: {ticker})")
            for row in stream_csv(csv_file):
                # Добавляем тикер и путь к данным строки (можно добавить и другие метаданные)
                message = {
                    'ticker': ticker,
                    'file': rel_path,
                    **row
                }
                # Отправляем в Kafka
                producer.send(TOPIC, value=message)
                print(f"Sent: {ticker} {list(row.values())[:3]}...")  # краткий лог
                delay = random.randint(DELAY_MIN, DELAY_MAX)
                time.sleep(delay)
            print(f"Finished file {rel_path}")
        print("All files processed. Restarting loop...")
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
    producer.close()