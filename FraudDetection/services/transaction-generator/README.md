# Transaction Generator Service

This service is responsible for simulating realistic banking transactions. It uses behavioral models for customers and merchants to generate a continuous stream of raw transaction events.

## Features
- Simulates Customers with realistic habits (preferred merchants, salaries, spending patterns).
- Simulates Merchants (categories, locations, business hours).
- Produces events to Kafka topic `bank.transactions.raw`.
- Controlled via REST API (FastAPI).

## Setup

```bash
poetry install
```

## Running

```bash
poetry run uvicorn src.main:app --reload --port 8001
```
