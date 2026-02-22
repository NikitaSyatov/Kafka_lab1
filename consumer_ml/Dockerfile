FROM python:3.9-slim
RUN pip install kafka-python numpy
WORKDIR /app
COPY ml_consumer.py .
ENTRYPOINT ["python", "-u", "ml_consumer.py"]