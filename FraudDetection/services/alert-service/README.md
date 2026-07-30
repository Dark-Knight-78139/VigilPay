# Alert Service

This service consumes predictions, filters for fraudulent transactions, and publishes high-priority alerts to the `bank.fraud.alerts` topic.

## Setup
```bash
poetry install
```

## Running
```bash
poetry run uvicorn src.main:app --reload --port 8004
```
