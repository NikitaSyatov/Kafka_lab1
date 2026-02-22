import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json
import os
import time
from collections import defaultdict, deque
from streamlit_autorefresh import st_autorefresh

# ---------- Конфигурация из переменных окружения ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-0:9092").split(",")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-market")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "visualizer-group")
MAX_POINTS = 100  # максимальное количество точек на графике для одного тикера

# ---------- Инициализация состояния ----------
if 'price_history' not in st.session_state:
    # price_history[ticker] = deque of (timestamp, close_price)
    st.session_state.price_history = defaultdict(lambda: deque(maxlen=MAX_POINTS))

if 'consumer' not in st.session_state:
    # Создаём consumer один раз
    st.session_state.consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    st.session_state.consumer.poll(timeout_ms=1000)  # первоначальная подписка

# Автообновление страницы каждые 2 секунды
st_autorefresh(interval=2000, key="auto_refresh")

# ---------- Заголовок ----------
st.title("📈 Stock Market Data Visualization")
st.markdown("Real-time data from Kafka topic `stock-market`")

# ---------- Чтение новых сообщений из Kafka ----------
messages = st.session_state.consumer.poll(timeout_ms=500)
new_data = False

for tp, records in messages.items():
    for record in records:
        data = record.value
        ticker = data.get('ticker')
        close_price = data.get('Close') or data.get('close')
        if close_price is None:
            continue
        # Сохраняем timestamp (можно взять из Kafka или из данных)
        timestamp = pd.Timestamp.now()  # или из data['Date'] если есть
        st.session_state.price_history[ticker].append((timestamp, float(close_price)))
        new_data = True

# Если появились новые данные, обновляем графики
if new_data:
    st.session_state.last_update = time.time()

# ---------- Выбор тикера для отображения ----------
tickers = list(st.session_state.price_history.keys())
if not tickers:
    st.info("No data received yet. Waiting for messages...")
    st.stop()

selected_ticker = st.selectbox("Select ticker", tickers)

# ---------- Подготовка данных для выбранного тикера ----------
history = st.session_state.price_history[selected_ticker]
if len(history) == 0:
    st.warning(f"No data for {selected_ticker}")
    st.stop()

df = pd.DataFrame(history, columns=["timestamp", "close_price"])
df.set_index("timestamp", inplace=True)

# ---------- График ----------
st.subheader(f"Close price for {selected_ticker} (last {len(df)} points)")
fig = px.line(df, y="close_price", title=f"{selected_ticker} Real-Time")
st.plotly_chart(fig, use_container_width=True)

# ---------- Статистика ----------
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Current price", f"${df['close_price'].iloc[-1]:.2f}")
with col2:
    st.metric("Min", f"${df['close_price'].min():.2f}")
with col3:
    st.metric("Max", f"${df['close_price'].max():.2f}")

# ---------- Таблица последних 10 записей ----------
st.subheader("Recent data")
st.dataframe(df.tail(10))