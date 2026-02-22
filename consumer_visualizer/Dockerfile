FROM python:3.9-slim

RUN pip install streamlit kafka-python pandas plotly streamlit-autorefresh

WORKDIR /app
COPY visualize.py .

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "visualize.py", "--server.port=8501", "--server.address=0.0.0.0"]