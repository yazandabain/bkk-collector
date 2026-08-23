FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
COPY tests ./tests

# All data lives under /data, which docker-compose.yml mounts as a volume
# on the host -- so `docker compose down` / container restarts never touch it.
VOLUME ["/data"]

CMD ["python", "-u", "collector.py"]
