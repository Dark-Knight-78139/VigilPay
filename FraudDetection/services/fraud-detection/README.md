# Fraud Detection Service

This service consumes enriched banking transactions, runs a machine learning model to predict fraud probability, and publishes the prediction.

## Features
- ML inference wrapper using Strategy pattern.
- Dynamic model registry allowing live reloading of models via API.
- Publishes structured predictions with explanations.

## Setup
```bash
poetry install
```

## Running
```bash
poetry run uvicorn src.main:app --reload --port 8003
```
