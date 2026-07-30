# Real-Time Fraud Detection Platform

This repository contains an enterprise-grade distributed machine learning platform for real-time fraud detection.

## Architecture

The system is designed around a microservices architecture using Event-Driven principles, with Kafka as the event backbone. 

### Core Components
- **Transaction Generator**: Simulates realistic banking customers, merchants, and transactions.
- **Feature Engineering**: Stateful real-time feature computation.
- **Fraud Detection**: Machine Learning inference service.
- **Alert Service**: Monitors predictions and generates alerts.
- **Monitoring Service**: Tracks system health, data drift, and model metrics.

## Infrastructure Setup

You need Docker and Docker Compose installed.

To start the Kafka cluster (KRaft mode):
```bash
cd infrastructure/docker
docker compose up -d
```

## Development

The project uses Python 3.12+ and Poetry for dependency management. Each microservice is independently managed within the `services/` directory.

### Standards
- Code formatting: `black`, `isort`
- Linting: `ruff`
- Type checking: `mypy`
- API Framework: `FastAPI`
- Events: `Pydantic v2`
