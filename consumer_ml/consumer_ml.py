#!/usr/bin/env python3
import os
import json
import logging
import time
from collections import defaultdict, deque

import numpy as np
import lightgbm as lgb
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ml-consumer')

# ---------- Конфигурация из переменных окружения ----------
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-0:9092,kafka-1:9092').split(',')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'ml-input')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'ml-result')
GROUP_ID = os.getenv('GROUP_ID', 'ml-group')

WINDOW_SIZE = int(os.getenv('WINDOW_SIZE', '10'))               # размер окна для признаков (лаги)
MIN_TRAIN_SAMPLES = int(os.getenv('MIN_TRAIN_SAMPLES', '100'))  # мин. точек для первого обучения
MAX_HISTORY = int(os.getenv('MAX_HISTORY', '500'))              # макс. хранимых точек на тикер
PREDICTION_STEPS = int(os.getenv('PREDICTION_STEPS', '3'))      # сколько шагов предсказывать
RETRAIN_EVERY = int(os.getenv('RETRAIN_EVERY', '50'))           # переобучать каждые N новых сообщений

# Гиперпараметры LightGBM (можно менять через окружение)
LGB_PARAMS = {
    'n_estimators': int(os.getenv('LGB_N_ESTIMATORS', '100')),
    'max_depth': int(os.getenv('LGB_MAX_DEPTH', '5')),
    'learning_rate': float(os.getenv('LGB_LEARNING_RATE', '0.1')),
    'subsample': float(os.getenv('LGB_SUBSAMPLE', '0.8')),
    'colsample_bytree': float(os.getenv('LGB_COLSAMPLE_BYTREE', '0.8')),
    'num_leaves': int(os.getenv('LGB_NUM_LEAVES', '31')),
    'random_state': 42,
    'n_jobs': 1,
    'verbose': -1  # отключаем лишние логи LightGBM
}

# ---------- Глобальные структуры данных ----------
price_history = defaultdict(lambda: deque(maxlen=MAX_HISTORY))   # сырые цены
models = {}                                                      # обученные модели по тикерам
is_trained = defaultdict(bool)                                   # флаг первого обучения
msg_count_since_train = defaultdict(int)                         # счётчик сообщений после последнего обучения

# ---------- Вспомогательные функции ----------
def create_consumer():
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info("Kafka consumer created")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"No brokers available, retrying {attempt+1}/10...")
            time.sleep(5)
    raise Exception("Could not create consumer after retries")

def create_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer created")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"No brokers available for producer, retrying {attempt+1}/10...")
            time.sleep(5)
    raise Exception("Could not create producer after retries")

def extract_features(prices_window):
    """
    Из массива цен (длиной WINDOW_SIZE) строит вектор признаков.
    Возвращает одномерный numpy массив.
    """
    prices = np.array(prices_window)
    features = []
    # 1. Сами цены (лаги)
    features.extend(prices)
    # 2. Статистики
    features.append(np.mean(prices))
    features.append(np.std(prices))
    features.append(np.min(prices))
    features.append(np.max(prices))
    # 3. Доходности (returns)
    if len(prices) > 1:
        returns = np.diff(prices) / prices[:-1]
        features.append(np.mean(returns))
        features.append(np.std(returns))
    else:
        features.extend([0.0, 0.0])
    return np.array(features)

def prepare_training_data(ticker):
    """
    По истории цен для тикера формирует матрицу X (признаки для каждого окна)
    и вектор y (целевые значения – следующая цена).
    Возвращает (X, y) или (None, None), если данных недостаточно.
    """
    history = list(price_history[ticker])
    if len(history) < WINDOW_SIZE + 1:
        return None, None
    X, y = [], []
    for i in range(WINDOW_SIZE, len(history)):
        window = history[i-WINDOW_SIZE:i]
        features = extract_features(window)
        X.append(features)
        y.append(history[i])
    return np.array(X), np.array(y)

def train_model(ticker):
    """Обучает модель LightGBM для указанного тикера на всех доступных данных."""
    X, y = prepare_training_data(ticker)
    if X is None or len(X) == 0:
        logger.warning(f"Not enough data to train model for {ticker}")
        return None
    model = lgb.LGBMRegressor(**LGB_PARAMS)
    model.fit(X, y)
    logger.info(f"Trained LightGBM for {ticker} on {len(X)} samples")
    return model

def predict_multi_step(ticker, steps=PREDICTION_STEPS):
    """Предсказывает следующие steps значений цены для тикера рекурсивно."""
    if ticker not in models:
        return None
    history = list(price_history[ticker])
    if len(history) < WINDOW_SIZE:
        return None
    # Берём последнее окно
    current_window = history[-WINDOW_SIZE:]
    predictions = []
    for _ in range(steps):
        # Извлекаем признаки из текущего окна
        features = extract_features(current_window).reshape(1, -1)
        pred = models[ticker].predict(features)[0]
        predictions.append(pred)
        # Обновляем окно: сдвигаем и добавляем предсказанное значение
        current_window.pop(0)
        current_window.append(pred)
    return predictions

# ---------- Основной цикл ----------
def main():
    logger.info(f"Starting ML consumer with LightGBM, steps={PREDICTION_STEPS}, retrain_every={RETRAIN_EVERY}")
    consumer = create_consumer()
    producer = create_producer()

    try:
        for msg in consumer:
            data = msg.value
            ticker = data.get('ticker')
            close_price = data.get('close_price')
            if not ticker or close_price is None:
                logger.debug(f"Skipping message without ticker/price: {data}")
                continue
            try:
                close_price = float(close_price)
            except (TypeError, ValueError):
                logger.warning(f"Invalid close_price: {close_price}")
                continue

            # Сохраняем цену в историю
            price_history[ticker].append(close_price)
            logger.debug(f"Received {ticker}: {close_price}")

            # --- Обучение / переобучение ---
            if not is_trained[ticker]:
                # Первое обучение
                if len(price_history[ticker]) >= MIN_TRAIN_SAMPLES:
                    model = train_model(ticker)
                    if model:
                        models[ticker] = model
                        is_trained[ticker] = True
                        msg_count_since_train[ticker] = 0
                        logger.info(f"First model trained for {ticker}")
            else:
                # Модель уже есть – считаем сообщения и при необходимости переобучаем
                msg_count_since_train[ticker] += 1
                if msg_count_since_train[ticker] >= RETRAIN_EVERY:
                    logger.info(f"Retraining model for {ticker} after {RETRAIN_EVERY} messages")
                    new_model = train_model(ticker)
                    if new_model:
                        models[ticker] = new_model
                        msg_count_since_train[ticker] = 0
                        logger.info(f"Model retrained for {ticker}")
                    else:
                        # Если не удалось переобучить (недостаточно данных), сбрасывать счётчик не будем
                        logger.warning(f"Retraining failed for {ticker}, will retry later")

            # --- Предсказание (если модель обучена) ---
            if is_trained[ticker]:
                predictions = predict_multi_step(ticker)
                if predictions:
                    logger.info(f"🔮 Predictions for {ticker}: {[round(p,2) for p in predictions]}")
                    result_message = {
                        'ticker': ticker,
                        'predictions': [round(p, 2) for p in predictions],
                        'steps': PREDICTION_STEPS,
                        'timestamp': time.time()
                    }
                    producer.send(OUTPUT_TOPIC, value=result_message)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == '__main__':
    main()