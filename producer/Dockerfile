FROM python:3.9-slim
RUN pip install kafka-python
WORKDIR /app
COPY producer.py .
ENTRYPOINT ["python", "-u", "producer.py"]