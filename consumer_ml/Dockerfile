FROM python:3.9-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*
RUN pip install kafka-python numpy scikit-learn lightgbm
WORKDIR /app
COPY consumer_ml.py .
ENTRYPOINT ["python", "-u", "consumer_ml.py"]