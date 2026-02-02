FROM python:3.10-slim

WORKDIR /app
COPY . /app

ENV PYTHONPATH=/app

RUN pip install fastapi uvicorn sqlalchemy aiosqlite prometheus-client opentelemetry-api opentelemetry-sdk

EXPOSE 8000
