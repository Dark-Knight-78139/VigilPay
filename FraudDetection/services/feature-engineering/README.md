# Feature Engineering Service

This service consumes raw banking transactions, computes real-time behavioral features using a Redis state store, and publishes enriched transactions.

## Features Computed
- **Velocity (1h)**: Number of transactions by the customer in the last hour.
- **Average Amount (24h)**: Rolling average of transaction amounts over the last 24 hours.
- **Country Change**: Flag indicating if the current transaction's location differs from the previous one.

## Setup
```bash
poetry install
```

## Running
```bash
poetry run uvicorn src.main:app --reload --port 8002
```
