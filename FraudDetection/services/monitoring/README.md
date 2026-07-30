# Monitoring Service

This MLOps service tracks model drift and data drift by consuming prediction events and exposing streaming aggregations as Prometheus metrics.

## Setup
```bash
poetry install
```

## Running
```bash
poetry run uvicorn src.main:app --reload --port 8005
```
