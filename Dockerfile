FROM python:3.9-slim

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install sqlite-web

COPY scripts/ ./scripts/
COPY data/ ./data/
COPY logs/ ./logs/

ENV LOG_FILE=./logs/etl.log

EXPOSE 8080
