from prometheus_client import Counter, Histogram, Gauge

# System metrics
MESSAGES_PROCESSED = Counter(
    'monitoring_messages_processed_total',
    'Total number of prediction messages processed',
    ['model_version']
)

# Model performance / drift metrics
MODEL_LATENCY = Histogram(
    'model_inference_latency_ms',
    'Latency of ML inference in milliseconds',
    ['model_version'],
    buckets=[1, 5, 10, 50, 100, 250, 500, 1000]
)

FRAUD_PREDICTIONS = Counter(
    'model_fraud_predictions_total',
    'Total number of transactions flagged as fraud',
    ['model_version']
)

AVG_PROBABILITY = Gauge(
    'model_avg_fraud_probability',
    'Rolling average fraud probability output by the model',
    ['model_version']
)

class DriftMonitor:
    def __init__(self):
        self.prob_sum = 0.0
        self.prob_count = 0

    def record_prediction(self, model_version: str, probability: float, is_fraud: bool, latency_ms: float):
        MESSAGES_PROCESSED.labels(model_version=model_version).inc()
        MODEL_LATENCY.labels(model_version=model_version).observe(latency_ms)
        
        if is_fraud:
            FRAUD_PREDICTIONS.labels(model_version=model_version).inc()
            
        # Update streaming average probability (simplistic implementation for demo)
        self.prob_sum += probability
        self.prob_count += 1
        avg_prob = self.prob_sum / self.prob_count
        
        AVG_PROBABILITY.labels(model_version=model_version).set(avg_prob)

drift_monitor = DriftMonitor()
