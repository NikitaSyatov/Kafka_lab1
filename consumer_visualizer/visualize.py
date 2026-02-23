import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import os
import time
from collections import defaultdict, deque
from streamlit_autorefresh import st_autorefresh
import numpy as np
from sklearn.metrics import r2_score

# ---------- Конфигурация ----------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-0:9092,kafka-1:9092").split(",")
STOCK_TOPIC = os.getenv("KAFKA_TOPIC", "stock-market")
ML_TOPIC = os.getenv("ML_TOPIC", "ml-result")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "visualizer-group")
MAX_POINTS = 200
WINDOW_R2 = 30  # размер окна для скользящего R²

# Инициализация состояния
if 'price_history' not in st.session_state:
    st.session_state.price_history = defaultdict(lambda: deque(maxlen=MAX_POINTS))
    st.session_state.counter = defaultdict(int)

if 'prediction_history' not in st.session_state:
    st.session_state.prediction_history = defaultdict(list)

if 'actual_vs_pred' not in st.session_state:
    st.session_state.actual_vs_pred = defaultdict(list)

if 'accuracy_history' not in st.session_state:
    st.session_state.accuracy_history = defaultdict(list)

if 'consumer' not in st.session_state:
    st.session_state.consumer = KafkaConsumer(
        STOCK_TOPIC,
        ML_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    st.session_state.consumer.poll(timeout_ms=1000)

# Автообновление каждые 2 секунды
st_autorefresh(interval=2000, key="auto_refresh")

st.title("📈 Stock Market with Prediction Accuracy")
st.markdown("Real-time data from Kafka: **stock-market** (prices) and **ml-result** (predictions)")

# ---------- Чтение сообщений из Kafka ----------
messages = st.session_state.consumer.poll(timeout_ms=500)
new_data = False

def update_r2(ticker):
    """Обновляет историю R² для тикера на основе последних пар."""
    pairs = st.session_state.actual_vs_pred[ticker]
    if len(pairs) < 2:
        return
    window = pairs[-WINDOW_R2:]
    actual = [p[0] for p in window]
    pred = [p[1] for p in window]
    r2 = r2_score(actual, pred)
    idx = len(st.session_state.accuracy_history[ticker])
    st.session_state.accuracy_history[ticker].append((idx, r2))

for tp, records in messages.items():
    topic = tp.topic
    for record in records:
        data = record.value
        if topic == STOCK_TOPIC:
            ticker = data.get('ticker')
            close_price = data.get('Close') or data.get('close')
            if close_price is None:
                continue
            idx = st.session_state.counter[ticker]
            st.session_state.price_history[ticker].append((idx, float(close_price)))
            st.session_state.counter[ticker] += 1
            new_data = True

            # Проверяем предсказание для этого индекса
            pred_list = [p for p in st.session_state.prediction_history[ticker] if p[0] == idx]
            if pred_list:
                real = float(close_price)
                pred = pred_list[0][1]
                st.session_state.actual_vs_pred[ticker].append((real, pred))
                update_r2(ticker)  # обновляем график точности

        elif topic == ML_TOPIC:
            ticker = data.get('ticker')
            predictions = data.get('predictions')
            steps = data.get('steps', 3)
            if ticker and predictions:
                last_idx = st.session_state.counter[ticker] - 1
                for i, pred in enumerate(predictions):
                    pred_idx = last_idx + 1 + i
                    st.session_state.prediction_history[ticker].append((pred_idx, float(pred)))
                    # Проверяем, не пришло ли реальное значение
                    real_list = [r for r in st.session_state.price_history[ticker] if r[0] == pred_idx]
                    if real_list:
                        real = real_list[0][1]
                        st.session_state.actual_vs_pred[ticker].append((real, float(pred)))
                        update_r2(ticker)
                new_data = True

if new_data:
    st.session_state.last_update = time.time()

# ---------- Выбор тикера ----------
all_tickers = set(st.session_state.price_history.keys()) | set(st.session_state.prediction_history.keys())
tickers = sorted(list(all_tickers))
if not tickers:
    st.info("No data received yet. Waiting for messages...")
    st.stop()

selected_ticker = st.selectbox("Select ticker", tickers)

# ---------- Основной график (цены и предсказания) ----------
fig1 = go.Figure()

real_history = list(st.session_state.price_history.get(selected_ticker, []))
if real_history:
    df_real = pd.DataFrame(real_history, columns=["index", "price"])
    fig1.add_trace(go.Scatter(
        x=df_real["index"],
        y=df_real["price"],
        mode='lines',
        name='Historical close price',
        line=dict(color='blue')
    ))

pred_history = st.session_state.prediction_history.get(selected_ticker, [])
if pred_history:
    df_pred = pd.DataFrame(pred_history, columns=["index", "predicted_price"])
    df_pred = df_pred.sort_values("index")
    fig1.add_trace(go.Scatter(
        x=df_pred["index"],
        y=df_pred["predicted_price"],
        mode='lines',
        name='Predicted prices',
        line=dict(color='red', dash='dash'),
    ))

fig1.update_layout(
    title=f"{selected_ticker} - Real Prices vs Predictions",
    xaxis_title="Message Index",
    yaxis_title="Price",
    hovermode='x unified'
)
st.plotly_chart(fig1, use_container_width=True)

# ---------- График точности (R² скользящее окно) ----------
if st.session_state.accuracy_history[selected_ticker]:
    df_acc = pd.DataFrame(st.session_state.accuracy_history[selected_ticker], columns=["index", "r2"])
    fig_acc = go.Figure()
    fig_acc.add_trace(go.Scatter(
        x=df_acc["index"],
        y=df_acc["r2"] * 100,
        mode='lines+markers',
        name=f'R² (window={WINDOW_R2})',
        line=dict(color='green')
    ))
    fig_acc.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="100%")
    fig_acc.update_layout(
        title=f"{selected_ticker} - Prediction Accuracy (R², last {WINDOW_R2} points)",
        xaxis_title="Update step",
        yaxis_title="R² (%)",
        yaxis=dict(range=[min(-50, df_acc["r2"].min()*100 - 10), 110])
    )
    st.plotly_chart(fig_acc, use_container_width=True)

    # Дополнительная метрика: средний R² за всё время
    overall_r2 = r2_score(
        [p[0] for p in st.session_state.actual_vs_pred[selected_ticker]],
        [p[1] for p in st.session_state.actual_vs_pred[selected_ticker]]
    )
    st.metric("Overall R²", f"{overall_r2:.3f}")
else:
    st.info("Not enough data to calculate accuracy yet. Waiting for matched predictions...")

# ---------- Метрики текущего состояния ----------
col1, col2 = st.columns(2)
with col1:
    if real_history:
        current_price = real_history[-1][1]
        st.metric("Current price", f"${current_price:.2f}")
    else:
        st.metric("Current price", "N/A")
with col2:
    if pred_history:
        last_pred = pred_history[-1][1]
        st.metric("Latest prediction", f"${last_pred:.2f}")
    else:
        st.metric("Latest prediction", "N/A")

# ---------- Таблицы ----------
if real_history:
    st.subheader("Recent historical prices")
    df_real_tail = pd.DataFrame(real_history[-10:], columns=["index", "price"])
    st.dataframe(df_real_tail)

if pred_history:
    st.subheader("Prediction history (last 10)")
    df_pred_tail = pd.DataFrame(pred_history[-10:], columns=["index", "predicted_price"])
    st.dataframe(df_pred_tail)