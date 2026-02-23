echo "-----RUNNER KAFKA-----"

echo "First phase: download stock market dataset"
python3 download_data.py

echo "Second phase: build and run kafka server"
docker-compose up -d

echo "Running status:"
docker-compose ps

echo "

View vizualization on http://0.0.0.0:8501


"

echo "For delete dockers, use command: $ docker-compose down -v"