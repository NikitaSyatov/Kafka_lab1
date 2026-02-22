FROM python:3.9-slim
RUN pip install kafka-python
WORKDIR /app
COPY data_processing.py .
ENTRYPOINT ["python", "-u", "data_processing.py"]